from __future__ import annotations

import asyncio
import json
import stat
from contextlib import asynccontextmanager
from types import SimpleNamespace

import httpx
import pytest
from meshcall import Mesh, MeshCallError, Router
from pydantic import BaseModel, ConfigDict

from connector.control import ConnectorController
from connector.core.config import ConnectorConfig
from connector.management import service as shared
from connector.management.doctor import diagnose
from connector.mesh.control import ControlEvents
from connector.mesh.hub import (
    CONTROL_SERVICE,
    DSH_SYNC_SERVICE,
    SHARED_SERVICE,
    LocalHub,
)
from connector.mesh.logs import LogHub, LogRecord, LogsService


class Anything(BaseModel):
    model_config = ConfigDict(extra="allow")


class Backend:
    """A backend that stays connected; the service, not the test, owns the runtime."""

    def __init__(self, config: ConnectorConfig) -> None:
        self.config = config

    async def run_forever(self) -> None:
        await asyncio.Event().wait()


def controller_at(tmp_path, configured: bool = True) -> ConnectorController:
    path = tmp_path / "connector.json"
    if configured:
        ConnectorConfig(
            server_url="https://aa.example.test",
            connector_id="existing",
            connector_token="DEVICE-SECRET",
        ).save(path)
    return ConnectorController(config_path=path, client_factory=Backend)


class Manager:
    """A local manager: the same role the DSH plugin holds."""

    def __init__(self, mesh: Mesh) -> None:
        self.mesh = mesh

    async def __call__(self, method: str, **params) -> dict:
        result = await self.mesh.call_unary(
            SHARED_SERVICE, method, Anything(**params), Anything, timeout=10
        )
        return result.model_dump()


def router_at(path) -> Router:
    endpoint = json.loads(path.read_text())["endpoint"]
    if "unixPath" in endpoint:
        return Router(unix_path=endpoint["unixPath"])
    return Router(pipe=endpoint["pipe"])


@asynccontextmanager
async def manager_at(path):
    token = json.loads(path.read_text())["key"]
    async with Mesh(router_at(path), token=token) as mesh:
        yield Manager(mesh)


@asynccontextmanager
async def shared_service(tmp_path, controller: ConnectorController, *, logs=None):
    """The service as ``anywhere-cli serve`` runs it, without signal handlers."""
    events = ControlEvents()
    log_hub = logs or LogHub()
    hub = LocalHub(directory=tmp_path / "router")
    path = tmp_path / "connector-service"
    await hub.start()
    try:
        await hub.mount(
            shared.SharedService(controller, events, asyncio.get_running_loop()),
            LogsService(log_hub),
        )
        shared.publish_rendezvous(path, hub)
        if controller.config_path.exists():
            await controller.start()
        yield SimpleNamespace(path=path, hub=hub, controller=controller, logs=log_hub)
    finally:
        shared.withdraw_rendezvous(path, hub.instance_id)
        await controller.shutdown()
        await hub.stop()


def account_transport(monkeypatch, handler):
    client_type = httpx.AsyncClient
    monkeypatch.setattr(
        shared.httpx,
        "AsyncClient",
        lambda **kwargs: client_type(transport=httpx.MockTransport(handler), **kwargs),
    )


def test_managers_share_one_runtime_and_leaving_does_not_stop_it(tmp_path):
    async def exercise():
        controller = controller_at(tmp_path)
        async with shared_service(tmp_path, controller) as service:
            mode = stat.S_IMODE(service.path.stat().st_mode)
            assert mode == 0o600, oct(mode)
            assert stat.S_IMODE(service.path.parent.stat().st_mode) == 0o700
            report = await diagnose(service.path, ["dsh", "codex"])
            assert report["ok"], report
            runtime = controller._runtime_task
            before = controller.config_path.read_bytes()
            async with manager_at(service.path) as manager:
                state = await manager("getState")
                assert state["running"] and state["status"] == "running"
                identity = await manager("getIdentity")
                assert identity["connectorId"] == "existing"
                assert "SECRET" not in json.dumps(identity)
                # A manager cannot rewrite the shared configuration.
                with pytest.raises(MeshCallError) as denied:
                    await manager("saveConfig")
                assert denied.value.code == "method_not_found"
            await asyncio.sleep(0)
            assert controller._runtime_task is runtime
            assert not runtime.done()
            assert controller.config_path.read_bytes() == before
            async with manager_at(service.path) as manager:
                stopped = await manager("stop")
                assert not stopped["running"]
                started = await manager("start")
                assert started["running"]
        assert not service.path.exists()
        assert runtime.done()

    asyncio.run(exercise())


def test_manager_key_cannot_reach_other_roles_services(tmp_path):
    async def exercise():
        controller = controller_at(tmp_path, configured=False)
        async with shared_service(tmp_path, controller) as service, manager_at(service.path) as manager:
            with pytest.raises(MeshCallError) as control:
                await manager.mesh.call_unary(
                    CONTROL_SERVICE, "getState", Anything(), Anything, timeout=10
                )
            assert control.value.code == "permission_denied"
            with pytest.raises(MeshCallError) as sync:
                await manager.mesh.call_unary(
                    DSH_SYNC_SERVICE, "ingestBatch", Anything(), Anything, timeout=10
                )
            assert sync.value.code == "permission_denied"

    asyncio.run(exercise())


def test_the_service_keeps_runtime_logs_for_manager_reads(tmp_path):
    async def exercise():
        controller = controller_at(tmp_path, configured=False)
        async with shared_service(tmp_path, controller) as service:
            service.logs.append(
                [LogRecord(time="2026-10-27T00:00:00Z", level="INFO", name="connector", message="started")],
                "connector",
            )
            async with manager_at(service.path) as manager:
                page = await manager.mesh.call_unary(
                    "aa.local.v2.Logs", "read", Anything(), Anything, timeout=10
                )
                messages = [item["message"] for item in page.model_dump()["items"]]
                assert "started" in messages

    asyncio.run(exercise())


def test_account_attachment_reuses_identity_and_never_rotates_valid_credentials(tmp_path, monkeypatch):
    calls = []

    def handler(request):
        calls.append((request.method, request.url.path))
        if request.url.path.endswith("/auth/me"):
            return httpx.Response(200, json={"userId": "owner"})
        if request.url.path.endswith("/connectors/existing"):
            return httpx.Response(
                200,
                json={"connector": {"id": "existing", "userId": "owner", "name": "Machine"}},
            )
        if request.url.path.endswith("/connector/auth"):
            return httpx.Response(200, json={})
        raise AssertionError(request.url)

    account_transport(monkeypatch, handler)

    async def exercise():
        controller = controller_at(tmp_path)
        before = controller.config_path.read_bytes()
        async with shared_service(tmp_path, controller) as service:
            task = controller._runtime_task
            async with manager_at(service.path) as manager:
                for _ in range(2):
                    identity = await manager(
                        "attachAccount",
                        serverUrl="https://aa.example.test",
                        userId="owner",
                        accessToken="OAUTH-SECRET",
                    )
                    assert identity["connectorId"] == "existing"
                    assert identity["name"] == "Machine"
                    assert "SECRET" not in json.dumps(identity)
            assert controller._runtime_task is task
            assert controller.config_path.read_bytes() == before
            assert all("revoke" not in path for _, path in calls)

    asyncio.run(exercise())


def test_foreign_account_cannot_take_over_a_configured_device(tmp_path, monkeypatch):
    def handler(request):
        if request.url.path.endswith("/auth/me"):
            return httpx.Response(200, json={"userId": "stranger"})
        return httpx.Response(
            200, json={"connector": {"id": "existing", "userId": "owner", "name": "Machine"}}
        )

    account_transport(monkeypatch, handler)

    async def exercise():
        controller = controller_at(tmp_path)
        before = controller.config_path.read_bytes()
        async with shared_service(tmp_path, controller) as service, manager_at(service.path) as manager:
            with pytest.raises(MeshCallError, match="归属"):
                    await manager(
                        "attachAccount",
                        serverUrl="https://aa.example.test",
                        userId="stranger",
                        accessToken="SECRET",
                    )
        assert controller.config_path.read_bytes() == before

    asyncio.run(exercise())


def test_first_registration_is_owned_by_the_service_and_idempotent(tmp_path, monkeypatch):
    registrations = []

    def handler(request):
        if request.url.path.endswith("/auth/me"):
            return httpx.Response(200, json={"userId": "owner"})
        if request.url.path == "/api/v2/connectors":
            registrations.append(json.loads(request.content)["installationId"])
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
        controller = controller_at(tmp_path, configured=False)
        async with shared_service(tmp_path, controller) as service:
            async with manager_at(service.path) as manager:
                params = {
                    "serverUrl": "https://aa.example.test",
                    "userId": "owner",
                    "accessToken": "OAUTH-SECRET",
                }
                with pytest.raises(MeshCallError):
                    await manager("attachAccount", **params)
                identity = await manager("attachAccount", **params)
                assert identity["connectorId"] == "new"
                assert registrations[0] == registrations[1]
            config = ConnectorConfig.load(controller.config_path)
            assert config.connector_token == "NEW-SECRET"
            mode = stat.S_IMODE(controller.config_path.stat().st_mode)
            assert mode == 0o600, oct(mode)

    asyncio.run(exercise())


def test_explicit_recovery_is_required_before_rotating_credentials(tmp_path, monkeypatch):
    revoked = []

    def handler(request):
        if request.url.path.endswith("/auth/me"):
            return httpx.Response(200, json={"userId": "owner"})
        if request.url.path.endswith("/connectors/existing/revoke"):
            revoked.append(request.url.path)
            return httpx.Response(
                200,
                json={
                    "connector": {"id": "existing", "userId": "owner", "name": "Machine"},
                    "connectorToken": "RENEWED-SECRET",
                },
            )
        if request.url.path.endswith("/connectors/existing"):
            return httpx.Response(
                200, json={"connector": {"id": "existing", "userId": "owner", "name": "Machine"}}
            )
        if request.url.path.endswith("/connector/auth"):
            return httpx.Response(401, json={})
        raise AssertionError(request.url)

    account_transport(monkeypatch, handler)

    async def exercise():
        controller = controller_at(tmp_path)
        before = controller.config_path.read_bytes()
        async with shared_service(tmp_path, controller) as service:
            async with manager_at(service.path) as manager:
                with pytest.raises(MeshCallError) as expired:
                    await manager(
                        "attachAccount",
                        serverUrl="https://aa.example.test",
                        userId="owner",
                        accessToken="OAUTH-SECRET",
                    )
                assert expired.value.code == shared.CREDENTIAL_EXPIRED
                assert controller.config_path.read_bytes() == before
                identity = await manager(
                    "attachAccount",
                    serverUrl="https://aa.example.test",
                    userId="owner",
                    accessToken="OAUTH-SECRET",
                    recovery="reconnect",
                )
                assert identity["connectorId"] == "existing"
            assert revoked == ["/api/v2/connectors/existing/revoke"]
            assert ConnectorConfig.load(controller.config_path).connector_token == "RENEWED-SECRET"

    asyncio.run(exercise())


def test_cached_owner_blocks_foreign_recreation_even_when_server_hides_device(tmp_path, monkeypatch):
    def handler(request):
        if request.url.path.endswith("/auth/me"):
            return httpx.Response(200, json={"userId": "stranger"})
        return httpx.Response(404, json={})

    account_transport(monkeypatch, handler)

    async def exercise():
        controller = controller_at(tmp_path)
        shared.write_private_json(
            tmp_path / "connector-owner.json",
            {"serverUrl": "https://aa.example.test", "connectorId": "existing", "userId": "owner"},
        )
        before = controller.config_path.read_bytes()
        async with shared_service(tmp_path, controller) as service, manager_at(service.path) as manager:
            with pytest.raises(MeshCallError, match="其他账号"):
                    await manager(
                        "attachAccount",
                        serverUrl="https://aa.example.test",
                        userId="stranger",
                        accessToken="SECRET",
                        recovery="recreate",
                    )
        assert controller.config_path.read_bytes() == before

    asyncio.run(exercise())


def test_a_replaced_rendezvous_is_left_for_its_new_owner(tmp_path):
    async def exercise():
        first = LocalHub(directory=tmp_path / "router-one")
        second = LocalHub(directory=tmp_path / "router-two")
        path = tmp_path / "connector-service"
        await first.start()
        await second.start()
        try:
            shared.publish_rendezvous(path, first)
            shared.publish_rendezvous(path, second)
            shared.withdraw_rendezvous(path, first.instance_id)
            assert json.loads(path.read_text())["instanceId"] == second.instance_id
            shared.withdraw_rendezvous(path, second.instance_id)
            assert not path.exists()
        finally:
            await first.stop()
            await second.stop()

    asyncio.run(exercise())


@pytest.mark.parametrize("command", ["configure", "pair"])
def test_cli_credential_writers_cannot_bypass_the_shared_owner(tmp_path, monkeypatch, command):
    from connector import cli
    from connector.core.runtime_owner import RuntimeLease

    controller = controller_at(tmp_path)
    before = controller.config_path.read_bytes()

    def held(*args, **kwargs):
        raise RuntimeError("shared service owns this machine")

    monkeypatch.setattr(RuntimeLease, "claim", held)
    if command == "configure":
        args = cli._build_parser().parse_args([
            "configure",
            "--config", str(controller.config_path),
            "--server-url", "https://other.example.test",
            "--connector-id", "other",
            "--connector-token", "other-token",
        ])
        with pytest.raises(RuntimeError, match="owns"):
            cli._configure(args)
    else:
        args = cli._build_parser().parse_args([
            "pair", "https://other.example.test", "--no-start",
            "--config", str(controller.config_path),
        ])
        with pytest.raises(RuntimeError, match="owns"):
            asyncio.run(cli._pair(args))
    assert controller.config_path.read_bytes() == before
