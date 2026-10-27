from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

from connector.runtime_protocol import RuntimeProvider
from connector.runtimes import providers
from connector.server.runtime_sync import RuntimeSyncRunner


class ExtraProvider(RuntimeProvider):
    runtime_type = "extra"
    display_name = "Extra"


def entry(name, factory):
    return SimpleNamespace(name=name, value=name, load=lambda: factory)


def test_installed_extension_is_added_without_replacing_official_providers(monkeypatch):
    monkeypatch.setattr(
        providers, "entry_points", lambda **kwargs: [entry("extra", ExtraProvider)]
    )
    loaded = providers.default_runtime_providers()
    assert [item.runtime_type for item in loaded] == ["codex", "claude", "dsh", "extra"]
    assert type(loaded[0]) is providers.CodexProvider
    assert type(loaded[1]) is providers.ClaudeProvider
    assert type(loaded[2]) is providers.DshProvider


def test_bad_extensions_cannot_disable_or_override_official_runtimes(monkeypatch):
    class Duplicate(ExtraProvider):
        runtime_type = "dsh"

    def broken():
        raise ImportError("incompatible extension")

    monkeypatch.setattr(
        providers,
        "entry_points",
        lambda **kwargs: [
            entry("a-broken", broken),
            entry("b-duplicate", Duplicate),
            entry("c-invalid", object),
            entry("d-valid", ExtraProvider),
            entry("e-repeat", ExtraProvider),
        ],
    )
    loaded = providers.default_runtime_providers()
    assert [item.runtime_type for item in loaded] == ["codex", "claude", "dsh", "extra"]
    assert isinstance(loaded[2], providers.DshProvider)


def test_reconnect_reaches_polling_extensions_and_continues_after_failure():
    async def exercise():
        bad = SimpleNamespace(
            sync_mode="polling",
            on_backend_reconnect=AsyncMock(side_effect=RuntimeError("broken")),
        )
        pi = SimpleNamespace(sync_mode="polling", on_backend_reconnect=AsyncMock())
        event = SimpleNamespace(
            sync_mode="events",
            on_backend_reconnect=AsyncMock(),
            resynchronize=AsyncMock(),
        )
        runtimes = {"bad": bad, "pi": pi, "dsh": event}
        supervisor = SimpleNamespace(
            runtimes=runtimes, resolve_runtime=runtimes.__getitem__
        )
        runner = RuntimeSyncRunner(None, supervisor, None, dict, AsyncMock())
        await runner.reconnect_event_runtimes()
        pi.on_backend_reconnect.assert_awaited_once_with()
        event.resynchronize.assert_awaited_once_with()
        event.on_backend_reconnect.assert_awaited_once_with()

    asyncio.run(exercise())
