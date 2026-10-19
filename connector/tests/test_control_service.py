from __future__ import annotations

import asyncio
import json
import stat

import httpx
import pytest

from connector.control import ConnectorController
from connector.core.config import ConnectorConfig
from connector.core.json_rpc import JsonRpcError
from connector.logging import logger
from connector.management import service as control_service
from connector.management.doctor import diagnose


class Backend:
    def __init__(self, config):
        self.config = config

    async def run_forever(self):
        await asyncio.Event().wait()


def service_at(tmp_path, configured=True):
    path = tmp_path / "connector.json"
    if configured:
        ConnectorConfig(
            server_url="https://aa.example.test",
            connector_id="existing",
            connector_token="DEVICE-SECRET",
        ).save(path)
    controller = ConnectorController(config_path=path, client_factory=Backend)
    return control_service.ConnectorService(controller, tmp_path / "control.sock")


async def rpc(reader, writer, method, params=None, request_id=1):
    writer.write(
        json.dumps(
            {"jsonrpc": "2.0", "id": request_id, "method": method, "params": params}
        ).encode()
        + b"\n"
    )
    await writer.drain()
    while True:
        frame = json.loads(await asyncio.wait_for(reader.readline(), 3))
        if frame.get("id") == request_id:
            return frame


def test_clients_share_one_runtime_and_disconnect_does_not_stop_it(tmp_path):
    async def exercise():
        service = service_at(tmp_path)
        await service.start()
        original_task = service.controller._runtime_task
        before = service.controller.config_path.read_bytes()
        try:
            assert stat.S_IMODE(service.socket_path.stat().st_mode) == 0o600
            report = await diagnose(service.socket_path, ["dsh", "codex"])
            assert report["ok"], report
            logger.info("credential DEVICE-SECRET")
            logs = service.read_logs()
            assert logs["entries"] and "SECRET" not in json.dumps(logs)
            reader, writer = await asyncio.open_unix_connection(service.socket_path)
            state = await rpc(reader, writer, "connector.getState")
            assert (
                state["result"]["running"] and state["result"]["protocolVersion"] == 1
            )
            writer.close()
            await writer.wait_closed()
            await asyncio.sleep(0)
            reader, writer = await asyncio.open_unix_connection(service.socket_path)
            try:
                identity = await rpc(reader, writer, "connector.getIdentity")
                assert identity["result"]["connectorId"] == "existing"
                assert service.controller._runtime_task is original_task
                assert not original_task.done()
                assert b"SECRET" not in json.dumps(identity).encode()
                assert service.controller.config_path.read_bytes() == before
                # Clients cannot overwrite configuration through start or retrieve tokens.
                rejected = await rpc(
                    reader, writer, "connector.start", {"connectorToken": "new"}
                )
                assert rejected["error"]["code"] == -32602
                rejected = await rpc(reader, writer, "connector.getConfig")
                assert rejected["error"]["code"] == -32601
                stopped = await rpc(reader, writer, "connector.stop")
                assert not stopped["result"]["running"]
                started = await rpc(reader, writer, "connector.start")
                assert started["result"]["running"]
            finally:
                writer.close()
                await writer.wait_closed()
        finally:
            await service.close()
        assert not service.socket_path.exists()
        assert original_task.done()

    asyncio.run(exercise())


def account_transport(monkeypatch, handler):
    client_type = httpx.AsyncClient
    monkeypatch.setattr(
        control_service.httpx,
        "AsyncClient",
        lambda **kwargs: client_type(transport=httpx.MockTransport(handler), **kwargs),
    )


def test_account_attachment_reuses_identity_and_never_rotates_valid_credentials(
    tmp_path, monkeypatch
):
    calls = []

    def handler(request):
        calls.append((request.method, request.url.path))
        assert request.method == "GET" or request.url.path.endswith("/connector/auth")
        if request.url.path.endswith("/auth/me"):
            return httpx.Response(200, json={"userId": "owner"})
        if request.url.path.endswith("/connectors/existing"):
            return httpx.Response(
                200,
                json={
                    "connector": {
                        "id": "existing",
                        "userId": "owner",
                        "name": "Machine",
                    }
                },
            )
        return httpx.Response(200, json={})

    account_transport(monkeypatch, handler)

    async def exercise():
        service = service_at(tmp_path)
        await service.start()
        before = service.controller.config_path.read_bytes()
        task = service.controller._runtime_task
        try:
            account = {
                "serverUrl": "https://aa.example.test",
                "userId": "owner",
                "accessToken": "OAUTH-SECRET",
            }
            for _ in range(2):
                identity = await service.attach_account(account)
                assert identity == {"connectorId": "existing", "name": "Machine"}
                assert "SECRET" not in json.dumps(identity)
            assert service.controller._runtime_task is task
            assert service.controller.config_path.read_bytes() == before
            assert all("revoke" not in path for _, path in calls)
        finally:
            await service.close()

    asyncio.run(exercise())


def test_foreign_account_cannot_take_over_a_configured_device(tmp_path, monkeypatch):
    def handler(request):
        if request.url.path.endswith("/auth/me"):
            return httpx.Response(200, json={"userId": "stranger"})
        return httpx.Response(
            200,
            json={
                "connector": {"id": "existing", "userId": "owner", "name": "Machine"}
            },
        )

    account_transport(monkeypatch, handler)

    async def exercise():
        service = service_at(tmp_path)
        before = service.controller.config_path.read_bytes()
        with pytest.raises(JsonRpcError, match="归属"):
            await service.attach_account(
                {
                    "serverUrl": "https://aa.example.test",
                    "userId": "stranger",
                    "accessToken": "SECRET",
                }
            )
        assert service.controller.config_path.read_bytes() == before

    asyncio.run(exercise())


def test_first_registration_is_owned_by_the_service_and_idempotent(
    tmp_path, monkeypatch
):
    registrations = []

    def handler(request):
        if request.url.path.endswith("/auth/me"):
            return httpx.Response(200, json={"userId": "owner"})
        if request.url.path == "/api/v2/connectors":
            body = json.loads(request.content)
            registrations.append(body["installationId"])
            if len(registrations) == 1:
                raise httpx.ReadTimeout("response lost", request=request)
            return httpx.Response(
                200,
                json={
                    "connector": {"id": "new", "userId": "owner", "name": "Machine"},
                    "connectorToken": "NEW-SECRET",
                },
            )
        raise AssertionError(request.url)

    account_transport(monkeypatch, handler)

    async def exercise():
        service = service_at(tmp_path, configured=False)
        await service.start()
        account = {
            "serverUrl": "https://aa.example.test",
            "userId": "owner",
            "accessToken": "OAUTH-SECRET",
        }
        try:
            with pytest.raises(httpx.ReadTimeout):
                await service.attach_account(account)
            result = await service.attach_account(account)
            assert result["connectorId"] == "new"
            assert registrations[0] == registrations[1]
            assert (
                ConnectorConfig.load(service.controller.config_path).connector_token
                == "NEW-SECRET"
            )
            assert stat.S_IMODE(service.controller.config_path.stat().st_mode) == 0o600
        finally:
            await service.close()

    asyncio.run(exercise())


def test_stale_socket_cleanup_never_deletes_an_unrelated_file(tmp_path):
    async def exercise():
        service = service_at(tmp_path, configured=False)
        service.socket_path.write_text("keep")
        with pytest.raises(RuntimeError, match="non-socket"):
            await service.start()
        assert service.socket_path.read_text() == "keep"

    asyncio.run(exercise())


def test_socket_does_not_change_permissions_of_a_shared_parent(tmp_path):
    async def exercise():
        service = service_at(tmp_path)
        tmp_path.chmod(0o755)
        with pytest.raises(RuntimeError, match="private directory"):
            await service.start()
        assert stat.S_IMODE(tmp_path.stat().st_mode) == 0o755
        assert not service.socket_path.exists()

    asyncio.run(exercise())


def test_cached_owner_blocks_foreign_recreation_even_when_server_hides_device(
    tmp_path, monkeypatch
):
    async def exercise():
        service = service_at(tmp_path)
        service._write_private_json(
            tmp_path / "connector-owner.json",
            {
                "serverUrl": "https://aa.example.test",
                "connectorId": "existing",
                "userId": "owner",
            },
        )
        before = service.controller.config_path.read_bytes()
        with pytest.raises(JsonRpcError, match="其他账号"):
            await service.attach_account(
                {
                    "serverUrl": "https://aa.example.test",
                    "userId": "stranger",
                    "accessToken": "SECRET",
                    "recovery": "recreate",
                }
            )
        assert service.controller.config_path.read_bytes() == before

    asyncio.run(exercise())


@pytest.mark.parametrize("command", ["configure", "pair"])
def test_cli_credential_writers_cannot_bypass_the_shared_owner(
    tmp_path, monkeypatch, command
):
    from connector import cli
    from connector.core.runtime_owner import RuntimeLease

    service = service_at(tmp_path)
    before = service.controller.config_path.read_bytes()

    def held(*args, **kwargs):
        raise RuntimeError("shared service owns this machine")

    monkeypatch.setattr(RuntimeLease, "claim", held)
    if command == "configure":
        args = cli._build_parser().parse_args(
            [
                "configure",
                "--config",
                str(service.controller.config_path),
                "--server-url",
                "https://other.example.test",
                "--connector-id",
                "other",
                "--connector-token",
                "other-token",
            ]
        )
        with pytest.raises(RuntimeError, match="owns"):
            cli._configure(args)
    else:
        args = cli._build_parser().parse_args(
            [
                "pair",
                "https://other.example.test",
                "--no-start",
                "--config",
                str(service.controller.config_path),
            ]
        )
        with pytest.raises(RuntimeError, match="owns"):
            asyncio.run(cli._pair(args))
    assert service.controller.config_path.read_bytes() == before
