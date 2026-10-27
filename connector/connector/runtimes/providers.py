from __future__ import annotations

from importlib.metadata import entry_points

from connector.logging import logger
from connector.runtime_protocol import RuntimeProvider
from connector.runtimes.claude.provider import ClaudeProvider
from connector.runtimes.codex.provider import CodexProvider
from connector.runtimes.dsh.provider import DshProvider

RUNTIME_PROVIDER_ENTRY_POINT_GROUP = "agents_anywhere.runtime_providers.v1"


def default_runtime_providers() -> tuple[RuntimeProvider, ...]:
    providers: list[RuntimeProvider] = [
        CodexProvider(),
        ClaudeProvider(),
        DshProvider(),
    ]
    registered = {provider.runtime_type for provider in providers}
    for entry in sorted(
        entry_points(group=RUNTIME_PROVIDER_ENTRY_POINT_GROUP),
        key=lambda item: (item.name, item.value),
    ):
        try:
            provider = entry.load()()
            if not isinstance(provider, RuntimeProvider):
                raise TypeError("runtime extension must return a RuntimeProvider")
            runtime_type = provider.runtime_type
            if not isinstance(runtime_type, str) or not runtime_type.strip():
                raise ValueError("runtime extension must have a nonempty runtime type")
            if runtime_type in registered:
                raise ValueError(f"runtime type already registered: {runtime_type}")
        except Exception:  # noqa: BLE001 - one extension must not disable built-in runtimes.
            logger.exception(
                "runtime extension rejected name={} value={}", entry.name, entry.value
            )
            continue
        registered.add(runtime_type)
        providers.append(provider)
    return tuple(providers)
