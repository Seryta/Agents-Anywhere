"""``aa.local.v2.Shared``: the one Connector that serves every local Agent.

``anywhere-cli serve`` owns the backend connection, the device identity and the
Runtime leases, and publishes a private rendezvous so a local manager (the DSH
plugin) can attach to it instead of starting a Connector of its own. Closing a
manager never stops this service; only an explicit stop or restart does, and
that affects every Agent on the machine.
"""

from __future__ import annotations

import asyncio
import json
import signal
import socket
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import replace
from pathlib import Path
from typing import Any, TypeVar
from urllib.parse import quote, urlsplit
from uuid import uuid4

import httpx
from meshcall import (
    InvalidArgumentError,
    MeshCallError,
    mutates,
    query,
    service,
    stream,
)
from pydantic import BaseModel

from connector.control import ConnectorBusyError, ConnectorController
from connector.core import runtime_owner
from connector.core.config import ConnectorConfig
from connector.logging import install_rpc_log_sink, logger
from connector.mesh.control import ConnectorState, ControlEvent, ControlEvents
from connector.mesh.events import stream_items
from connector.mesh.hub import (
    SERVICE_ROLE,
    SHARED_SERVICE,
    LocalHub,
    run_on_loop,
    set_local_hub,
    write_private_json,
)
from connector.mesh.logs import LogsService, open_log_hub
from connector.server.urls import api_v2_url

T = TypeVar("T")

RENDEZVOUS_VERSION = 1
CONNECTOR_ERROR = "connector_error"
# The local manager answers these two by asking the user, exactly as the stdio
# channel's -32011/-32012 did: reuse the credentials or register a new device.
CREDENTIAL_EXPIRED = "credential_expired"
DEVICE_DELETED = "device_deleted"


def rendezvous_path() -> Path:
    """Where this service advertises itself; DSH_HOME and state roots do not move it."""
    return runtime_owner.system_home() / ".agents-anywhere" / "connector-service"


class ConnectorIdentity(BaseModel):
    serverUrl: str
    connectorId: str
    name: str | None = None


@service(SHARED_SERVICE, description="One Connector for every local Agent: state, identity, account binding and lifecycle.")
class SharedService:
    """What a local manager may do with the shared Connector.

    Nothing here starts or stops a Connector process: the service owns its own
    lifetime, and the manager only observes it or restarts the Runtime tasks.
    """

    def __init__(
        self,
        controller: ConnectorController,
        events: ControlEvents,
        loop: asyncio.AbstractEventLoop,
    ) -> None:
        self._controller = controller
        self._events = events
        self._loop = loop
        # Binding an account rewrites the device identity, so it must not
        # interleave with another binding or lifecycle command.
        self._operations = asyncio.Lock()

    async def _run(self, work: Callable[[], Awaitable[T] | T]) -> T:
        async def call() -> T:
            try:
                result = work()
                return await result if isinstance(result, Awaitable) else result
            except MeshCallError:
                raise
            except ConnectorBusyError as exc:
                raise MeshCallError("connector_busy", str(exc), retryable=False) from exc
            except ValueError as exc:
                raise InvalidArgumentError(str(exc)) from exc
            except Exception as exc:
                raise MeshCallError(CONNECTOR_ERROR, str(exc) or exc.__class__.__name__) from exc

        return await run_on_loop(self._loop, call())

    async def _serial(self, work: Callable[[], Awaitable[T]]) -> T:
        async def call() -> T:
            async with self._operations:
                return await work()

        return await self._run(call)

    @query
    async def getState(self) -> ConnectorState:
        """The runtime's status and the last error it reported."""
        return ConnectorState.model_validate(await self._run(self._controller.get_state))

    @query
    async def getIdentity(self) -> ConnectorIdentity:
        """The saved device identity, without its token; empty before the first binding."""
        return await self._run(self._identity)

    @mutates
    async def attachAccount(
        self,
        serverUrl: str,
        userId: str,
        accessToken: str,
        recovery: str | None = None,
    ) -> ConnectorIdentity:
        """Keep the saved identity for this account, or register this machine once."""
        if not all(isinstance(value, str) and value for value in (serverUrl, userId, accessToken)):
            raise InvalidArgumentError("Account parameters are required")
        if recovery not in (None, "reconnect", "recreate"):
            raise InvalidArgumentError("Invalid account recovery")
        return await self._serial(
            lambda: self._attach(serverUrl, userId, accessToken, recovery)
        )

    @mutates
    async def start(self) -> ConnectorState:
        """Start the runtime tasks with the saved configuration."""
        return ConnectorState.model_validate(await self._serial(self._controller.start))

    @mutates
    async def stop(self) -> ConnectorState:
        """Stop every runtime; the service and its device identity stay up."""
        return ConnectorState.model_validate(await self._serial(self._controller.stop))

    @mutates
    async def restart(self) -> ConnectorState:
        """Stop, then start the runtime tasks with the saved configuration."""
        return ConnectorState.model_validate(await self._serial(self._controller.restart))

    @stream
    async def events(self, since: int | None = None) -> AsyncIterator[ControlEvent]:
        """State events after since; read getState for the current state."""
        subscription = self._events.hub.subscribe(since)
        async for event in stream_items(subscription):
            kind, payload = event.item
            state = payload if kind == "state" and isinstance(payload, ConnectorState) else None
            yield ControlEvent(seq=event.seq, kind="state" if state is not None else "pairing", state=state)

    def _identity(self) -> ConnectorIdentity:
        path = self._controller.config_path
        if not path.exists():
            return ConnectorIdentity(serverUrl="", connectorId="")
        config = ConnectorConfig.load(path)
        return ConnectorIdentity(serverUrl=config.server_url, connectorId=config.connector_id)

    async def _attach(
        self,
        server_url: str,
        user_id: str,
        access_token: str,
        recovery: str | None,
    ) -> ConnectorIdentity:
        server = _server_origin(server_url)
        config = (
            ConnectorConfig.load(self._controller.config_path)
            if self._controller.config_path.exists()
            else None
        )
        if config is not None and config.server_url.rstrip("/") != server:
            raise MeshCallError(
                CONNECTOR_ERROR, "公共 Connector 已连接其他服务器，请先在公共服务中切换。"
            )
        owner_path = self._controller.config_path.with_name("connector-owner.json")
        owner = json.loads(owner_path.read_text()) if owner_path.exists() else None
        if (
            config is not None
            and isinstance(owner, dict)
            and owner.get("connectorId") == config.connector_id
            and owner.get("serverUrl") == server
            and owner.get("userId") != user_id
        ):
            raise MeshCallError(
                CONNECTOR_ERROR, "公共 Connector 已属于其他账号，请先在公共服务中切换。"
            )
        headers = {"Authorization": f"Bearer {access_token}"}
        async with httpx.AsyncClient(timeout=20, follow_redirects=False) as client:
            async def request(method: str, path: str, **kwargs: Any) -> httpx.Response:
                return await client.request(method, api_v2_url(server, path), **kwargs)

            me = await request("GET", "/auth/me", headers=headers)
            _check_response(me)
            if me.json().get("userId") != user_id:
                raise MeshCallError(CONNECTOR_ERROR, "账号归属与授权不一致。")
            device = None
            if config is not None:
                device = await self._verify_device(
                    request, config, headers, user_id, recovery, owner, server
                )
            if device is None:
                # Persist before registering: a lost response can retry the same installation.
                installation = self._controller.config_path.with_name(
                    "connector-installation.json"
                )
                saved = json.loads(installation.read_text()) if installation.exists() else None
                replaced = config.connector_id if config is not None else None
                if isinstance(saved, dict) and (
                    saved.get("serverUrl") != server or saved.get("userId") != user_id
                ):
                    raise MeshCallError(
                        CONNECTOR_ERROR, "公共 Connector 的设备注册属于其他账号或服务器。"
                    )
                if not isinstance(saved, dict) or saved.get("replacesConnectorId") != replaced:
                    saved = {
                        "installationId": str(uuid4()),
                        "name": socket.gethostname(),
                        "replacesConnectorId": replaced,
                        "serverUrl": server,
                        "userId": user_id,
                    }
                    write_private_json(installation, saved)
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
                _check_response(registered)
                body = registered.json()
                device = body.get("connector")
                if (
                    not isinstance(device, dict)
                    or device.get("userId") != user_id
                    or not device.get("id")
                    or not body.get("connectorToken")
                ):
                    raise MeshCallError(CONNECTOR_ERROR, "设备注册响应无效。")
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
                await self._controller.stop()
                self._save_config(config)
        write_private_json(
            owner_path,
            {"serverUrl": server, "connectorId": device["id"], "userId": user_id},
        )
        await self._controller.start()
        return ConnectorIdentity(
            serverUrl=server,
            connectorId=device["id"],
            name=device.get("name") or device["id"],
        )

    async def _verify_device(
        self,
        request: Callable[..., Awaitable[httpx.Response]],
        config: ConnectorConfig,
        headers: dict[str, str],
        user_id: str,
        recovery: str | None,
        owner: Any,
        server: str,
    ) -> dict[str, Any] | None:
        """The server's record of this device while the account still owns it."""
        response = await request(
            "GET", f"/connectors/{quote(config.connector_id, safe='')}", headers=headers
        )
        if response.status_code == 404:
            if recovery != "recreate":
                raise MeshCallError(
                    DEVICE_DELETED,
                    "本机设备已删除，请明确重新创建设备。",
                    details={"connectorId": config.connector_id},
                )
            if not (
                isinstance(owner, dict)
                and owner.get("connectorId") == config.connector_id
                and owner.get("serverUrl") == server
                and owner.get("userId") == user_id
            ):
                raise MeshCallError(
                    CONNECTOR_ERROR, "无法确认旧设备归属，请先在公共服务中重新配对。"
                )
            return None
        _check_response(response)
        device = response.json().get("connector")
        if (
            not isinstance(device, dict)
            or device.get("id") != config.connector_id
            or device.get("userId") != user_id
        ):
            raise MeshCallError(
                CONNECTOR_ERROR, "公共 Connector 的设备归属与当前账号不一致。"
            )
        auth = await request(
            "POST",
            "/connector/auth",
            headers={
                "Authorization": f"Connector {config.connector_id}:{config.connector_token}"
            },
        )
        if auth.status_code != 401:
            _check_response(auth)
            return device
        if recovery != "reconnect":
            raise MeshCallError(
                CREDENTIAL_EXPIRED,
                "本机设备凭据已失效，请明确恢复连接。",
                details={"connectorId": config.connector_id},
            )
        renewed = await request(
            "POST", f"/connectors/{quote(config.connector_id, safe='')}/revoke", headers=headers
        )
        _check_response(renewed)
        body = renewed.json()
        if body.get("connector", {}).get("id") != config.connector_id or not body.get(
            "connectorToken"
        ):
            raise MeshCallError(CONNECTOR_ERROR, "设备恢复响应无效。")
        await self._controller.stop()
        self._save_config(replace(config, connector_token=body["connectorToken"]))
        return device

    def _save_config(self, config: ConnectorConfig) -> None:
        path = self._controller.config_path
        temporary = path.with_name(f".{path.name}.{uuid4()}.tmp")
        try:
            config.save(temporary)
            temporary.replace(path)
        finally:
            temporary.unlink(missing_ok=True)


def _server_origin(value: str) -> str:
    server = value.rstrip("/")
    parsed = urlsplit(server)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username
        or parsed.query
        or parsed.fragment
    ):
        raise InvalidArgumentError("Invalid server URL")
    return server


def _check_response(response: httpx.Response) -> None:
    if not response.is_success:
        raise MeshCallError(
            CONNECTOR_ERROR, f"公共 Connector 授权请求失败（HTTP {response.status_code}）。"
        )


def publish_rendezvous(path: Path, hub: LocalHub) -> Path:
    """Advertise this Router and the manager key to the current user only."""
    write_private_json(
        path,
        {"version": RENDEZVOUS_VERSION, "protocol": "meshcall", **hub.descriptor(SERVICE_ROLE)},
    )
    logger.info("shared service rendezvous published path={}", path)
    return path


def withdraw_rendezvous(path: Path, instance_id: str) -> None:
    """Remove the advertisement, unless a newer service already replaced it."""
    try:
        current = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return
    if isinstance(current, dict) and current.get("instanceId") == instance_id:
        try:
            path.unlink()
        except OSError:
            logger.warning("shared service rendezvous could not be removed path={}", path)


async def run_service(
    config_path: str | Path | None,
    *,
    rendezvous: str | Path | None = None,
    debug: bool = False,
) -> None:
    """Serve the shared Connector until SIGINT/SIGTERM."""
    loop = asyncio.get_running_loop()
    path = Path(rendezvous) if rendezvous is not None else rendezvous_path()
    if not path.is_absolute():
        raise ValueError("shared service rendezvous path must be absolute")
    events = ControlEvents()
    logs = open_log_hub(runtime_owner.system_home() / ".agents-anywhere" / "logs")
    controller = ConnectorController(config_path=config_path, notifier=events.notify)
    controller.acquire_ownership()
    sink = install_rpc_log_sink(
        logs.notify, level="DEBUG" if debug else "INFO", remove_default_sink=True
    )
    hub = LocalHub()
    set_local_hub(hub)
    stopped = asyncio.Event()
    for signum in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(signum, stopped.set)
    published: Path | None = None
    try:
        await hub.start()
        await hub.mount(SharedService(controller, events, loop), LogsService(logs))
        published = publish_rendezvous(path, hub)
        if controller.config_path.exists():
            await controller.start()
        await stopped.wait()
    finally:
        for signum in (signal.SIGINT, signal.SIGTERM):
            loop.remove_signal_handler(signum)
        if published is not None:
            withdraw_rendezvous(published, hub.instance_id)
        set_local_hub(None)
        try:
            await controller.shutdown()
        finally:
            try:
                await hub.stop()
            finally:
                await sink.close()
                logs.close()
