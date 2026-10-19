"""Per-user Connector service; local clients never own the backend connection."""

from __future__ import annotations

import asyncio
import json
import os
import signal
import socket
import time
from collections import deque
from dataclasses import replace
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlsplit
from uuid import uuid4

import httpx

from connector.control import ConnectorController
from connector.core.config import ConnectorConfig
from connector.core.json_rpc import JsonRpcError, JsonRpcStdioServer
from connector.logging import logger
from connector.paths import connector_data_dir
from connector.server.urls import api_v2_url

MAX_FRAME_BYTES = 1024 * 1024


def default_socket_path() -> Path:
    return connector_data_dir() / "connector.sock"


class ConnectorService:
    def __init__(
        self, controller: ConnectorController, socket_path: str | Path
    ) -> None:
        self.controller = controller
        self.socket_path = Path(socket_path)
        if not self.socket_path.is_absolute():
            raise ValueError("Connector socket path must be absolute")
        self._clients: set[JsonRpcStdioServer] = set()
        self._tasks: set[asyncio.Task[Any]] = set()
        self._logs: deque[dict[str, Any]] = deque(maxlen=10000)
        self._log_sequence = time.time_ns() // 1000
        self._redactions: set[str] = set()
        self._log_sink: int | None = None
        self._operations = asyncio.Lock()
        self._server: asyncio.Server | None = None
        self._socket_inode: int | None = None
        self.controller.notifier = self.notify

    def get_state(self, _params: Any = None) -> dict[str, Any]:
        return {**self.controller.get_state(), "protocolVersion": 1}

    def get_identity(self, _params: Any = None) -> dict[str, str] | None:
        if not self.controller.config_path.exists():
            return None
        config = ConnectorConfig.load(self.controller.config_path)
        return {"serverUrl": config.server_url, "connectorId": config.connector_id}

    async def notify(self, method: str, params: Any) -> None:
        # Pairing notifications from the stdio Controller may contain credentials.
        if method != "connector/state":
            return
        for client in tuple(self._clients):
            try:
                async with asyncio.timeout(1):
                    await client.notify(method, self.get_state())
            except (OSError, TimeoutError):
                client.writer.close()
                self._clients.discard(client)

    def capture_log(self, message: Any) -> None:
        text = message.record["message"]
        for secret in self._redactions:
            text = text.replace(secret, "[REDACTED]")
        text = text.encode("utf-8")[:2048].decode("utf-8", errors="ignore")
        self._log_sequence += 1
        self._logs.append(
            {
                "id": self._log_sequence,
                "time": message.record["time"].isoformat(),
                "text": text,
            }
        )

    def read_logs(self, params: Any = None) -> dict[str, Any]:
        params = {} if params is None else params
        if (
            not isinstance(params, dict)
            or set(params) - {"before", "after"}
            or any(
                not isinstance(value, int) or isinstance(value, bool) or value < 0
                for value in params.values()
            )
        ):
            raise JsonRpcError(-32602, "Invalid log pagination parameters")
        rows = [
            row
            for row in self._logs
            if row["id"] < params.get("before", float("inf"))
            and row["id"] > params.get("after", -1)
        ]
        return {
            "entries": rows[:200] if "after" in params else rows[-200:],
            "hasMore": len(rows) > 200,
            "oldestId": self._logs[0]["id"] if self._logs else None,
            "newestId": self._logs[-1]["id"] if self._logs else None,
        }

    async def attach_account(self, params: Any) -> dict[str, str]:
        if not isinstance(params, dict):
            raise JsonRpcError(-32602, "Account parameters are required")
        server = params.get("serverUrl")
        token = params.get("accessToken")
        user_id = params.get("userId")
        recovery = params.get("recovery")
        if not all(
            isinstance(value, str) and value for value in (server, token, user_id)
        ) or recovery not in (None, "reconnect", "recreate"):
            raise JsonRpcError(-32602, "Invalid account parameters")
        self._redactions.add(token)
        server = server.rstrip("/")
        parsed = urlsplit(server)
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.hostname
            or parsed.username
            or parsed.query
            or parsed.fragment
        ):
            raise JsonRpcError(-32602, "Invalid server URL")
        config = (
            ConnectorConfig.load(self.controller.config_path)
            if self.controller.config_path.exists()
            else None
        )
        if config is not None and config.server_url.rstrip("/") != server:
            raise JsonRpcError(
                -32000, "公共 Connector 已连接其他服务器，请先在公共服务中切换。"
            )
        owner_path = self.controller.config_path.with_name("connector-owner.json")
        owner = json.loads(owner_path.read_text()) if owner_path.exists() else None
        if (
            config is not None
            and isinstance(owner, dict)
            and owner.get("connectorId") == config.connector_id
            and owner.get("serverUrl") == server
            and owner.get("userId") != user_id
        ):
            raise JsonRpcError(
                -32000, "公共 Connector 已属于其他账号，请先在公共服务中切换。"
            )
        headers = {"Authorization": f"Bearer {token}"}
        async with httpx.AsyncClient(timeout=20, follow_redirects=False) as client:

            async def request(method: str, path: str, **kwargs: Any) -> httpx.Response:
                response = await client.request(
                    method, api_v2_url(server, path), **kwargs
                )
                return response

            me = await request("GET", "/auth/me", headers=headers)
            self._check_response(me)
            if me.json().get("userId") != user_id:
                raise JsonRpcError(-32000, "账号归属与授权不一致。")
            device = None
            if config is not None:
                response = await request(
                    "GET",
                    f"/connectors/{quote(config.connector_id, safe='')}",
                    headers=headers,
                )
                if (
                    response.status_code == 404
                    and recovery == "recreate"
                    and not (
                        isinstance(owner, dict)
                        and owner.get("connectorId") == config.connector_id
                        and owner.get("serverUrl") == server
                        and owner.get("userId") == user_id
                    )
                ):
                    raise JsonRpcError(
                        -32000, "无法确认旧设备归属，请先在公共服务中重新配对。"
                    )
                if response.status_code == 404 and recovery != "recreate":
                    raise JsonRpcError(
                        -32012,
                        "本机设备已删除，请明确重新创建设备。",
                        {"connectorId": config.connector_id},
                    )
                if response.status_code != 404 or recovery != "recreate":
                    self._check_response(response)
                    device = response.json().get("connector")
                    if (
                        not isinstance(device, dict)
                        or device.get("id") != config.connector_id
                        or device.get("userId") != user_id
                    ):
                        raise JsonRpcError(
                            -32000, "公共 Connector 的设备归属与当前账号不一致。"
                        )
                    auth = await request(
                        "POST",
                        "/connector/auth",
                        headers={
                            "Authorization": f"Connector {config.connector_id}:{config.connector_token}"
                        },
                    )
                    if auth.status_code == 401:
                        if recovery != "reconnect":
                            raise JsonRpcError(
                                -32011,
                                "本机设备凭据已失效，请明确恢复连接。",
                                {"connectorId": config.connector_id},
                            )
                        renewed = await request(
                            "POST",
                            f"/connectors/{quote(config.connector_id, safe='')}/revoke",
                            headers=headers,
                        )
                        self._check_response(renewed)
                        body = renewed.json()
                        if body.get("connector", {}).get(
                            "id"
                        ) != config.connector_id or not body.get("connectorToken"):
                            raise JsonRpcError(-32000, "设备恢复响应无效。")
                        config = replace(config, connector_token=body["connectorToken"])
                        await self.controller.stop()
                        self._save_config(config)
                    else:
                        self._check_response(auth)
            if device is None:
                # Persist before registering: lost responses can retry the same installation.
                installation = self.controller.config_path.with_name(
                    "connector-installation.json"
                )
                saved = (
                    json.loads(installation.read_text())
                    if installation.exists()
                    else None
                )
                replaced = config.connector_id if config is not None else None
                if isinstance(saved, dict) and (
                    saved.get("serverUrl") != server or saved.get("userId") != user_id
                ):
                    raise JsonRpcError(
                        -32000, "公共 Connector 的设备注册属于其他账号或服务器。"
                    )
                if (
                    not isinstance(saved, dict)
                    or saved.get("replacesConnectorId") != replaced
                ):
                    saved = {
                        "installationId": str(uuid4()),
                        "name": socket.gethostname(),
                        "replacesConnectorId": replaced,
                        "serverUrl": server,
                        "userId": user_id,
                    }
                    self._write_private_json(installation, saved)
                registered = await request(
                    "POST",
                    "/connectors",
                    headers=headers,
                    json={
                        "name": saved["name"],
                        "connectorKind": "cli",
                        "installationId": saved["installationId"],
                    },
                )
                self._check_response(registered)
                body = registered.json()
                device = body.get("connector")
                if (
                    not isinstance(device, dict)
                    or device.get("userId") != user_id
                    or not device.get("id")
                    or not body.get("connectorToken")
                ):
                    raise JsonRpcError(-32000, "设备注册响应无效。")
                config = (
                    replace(
                        config,
                        connector_id=device["id"],
                        connector_token=body["connectorToken"],
                    )
                    if config is not None
                    else ConnectorConfig(
                        server_url=server,
                        connector_id=device["id"],
                        connector_token=body["connectorToken"],
                    )
                )
                await self.controller.stop()
                self._save_config(config)
            self._write_private_json(
                owner_path,
                {"serverUrl": server, "connectorId": device["id"], "userId": user_id},
            )
            await self.controller.start()
            return {
                "connectorId": device["id"],
                "name": device.get("name") or device["id"],
            }

    @staticmethod
    def _write_private_json(path: Path, value: dict[str, Any]) -> None:
        temporary = path.with_name(f".{path.name}.{uuid4()}.tmp")
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            with os.fdopen(descriptor, "w") as stream:
                json.dump(value, stream)
                stream.flush()
                os.fsync(stream.fileno())
            temporary.replace(path)
        finally:
            temporary.unlink(missing_ok=True)

    @staticmethod
    def _check_response(response: httpx.Response) -> None:
        if not response.is_success:
            raise JsonRpcError(
                -32000, f"公共 Connector 授权请求失败（HTTP {response.status_code}）。"
            )

    def _save_config(self, config: ConnectorConfig) -> None:
        self._redactions.add(config.connector_token)
        temporary = self.controller.config_path.with_name(
            f".{self.controller.config_path.name}.{uuid4()}.tmp"
        )
        try:
            config.save(temporary)
            temporary.replace(self.controller.config_path)
        finally:
            temporary.unlink(missing_ok=True)

    def _handlers(self) -> dict[str, Any]:
        async def operation(method: str, params: Any) -> Any:
            async with self._operations:
                if method == "attachAccount":
                    return await self.attach_account(params)
                if params not in (None, {}):
                    raise JsonRpcError(
                        -32602, "Shared operations use the saved configuration"
                    )
                return await getattr(self.controller, method)()

        handlers: dict[str, Any] = {
            "connector.getState": self.get_state,
            "connector.getIdentity": self.get_identity,
            "connector.readLogs": self.read_logs,
        }
        for method in ("start", "stop", "restart", "attachAccount"):

            async def handle(params: Any, name: str = method) -> Any:
                return await operation(name, params)

            handlers[f"connector.{method}"] = handle
        return handlers

    async def _accept(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        task = asyncio.current_task()
        if task is not None:
            self._tasks.add(task)
        rpc = JsonRpcStdioServer(reader, writer, self._handlers())
        self._clients.add(rpc)
        try:
            await rpc.serve_forever()
        except (OSError, ValueError):
            pass
        finally:
            self._clients.discard(rpc)
            writer.close()
            try:
                await writer.wait_closed()
            except OSError:
                pass
            if task is not None:
                self._tasks.discard(task)

    async def start(self) -> None:
        # Take the global process lease before removing a stale socket.
        self.controller.acquire_ownership()
        if self.controller.config_path.exists():
            self._redactions.add(
                ConnectorConfig.load(self.controller.config_path).connector_token
            )
        self._log_sink = logger.add(
            self.capture_log, format="{message}", backtrace=False, diagnose=False
        )
        try:
            self.socket_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            directory = self.socket_path.parent.stat()
            if directory.st_uid != os.getuid() or directory.st_mode & 0o077:
                raise RuntimeError(
                    "Connector socket requires a private directory owned by the current user"
                )
            if self.socket_path.exists() or self.socket_path.is_symlink():
                if self.socket_path.is_symlink() or not self.socket_path.is_socket():
                    raise RuntimeError(
                        "Connector socket path is occupied by a non-socket file"
                    )
                self.socket_path.unlink()
            self._server = await asyncio.start_unix_server(
                self._accept, path=self.socket_path, limit=MAX_FRAME_BYTES
            )
            self._socket_inode = self.socket_path.stat().st_ino
            self.socket_path.chmod(0o600)
            if self.controller.config_path.exists():
                await self.controller.start()
        except BaseException:
            await self.close()
            raise

    async def close(self) -> None:
        server, self._server = self._server, None
        if server is not None:
            server.close()
        for client in tuple(self._clients):
            client.writer.close()
        for task in tuple(self._tasks):
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        if server is not None:
            await server.wait_closed()
        await self.controller.shutdown()
        if self._log_sink is not None:
            logger.remove(self._log_sink)
            self._log_sink = None
        if self._socket_inode is not None:
            try:
                if self.socket_path.lstat().st_ino == self._socket_inode:
                    self.socket_path.unlink()
            except FileNotFoundError:
                pass
            self._socket_inode = None


async def run_service(config_path: str | Path, socket_path: str | Path) -> None:
    service = ConnectorService(
        ConnectorController(config_path=config_path), socket_path
    )
    stopped = asyncio.Event()
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, stopped.set)
    try:
        await service.start()
        await stopped.wait()
    finally:
        loop.remove_signal_handler(signal.SIGTERM)
        await service.close()
