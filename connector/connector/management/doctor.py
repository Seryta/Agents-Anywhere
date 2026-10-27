"""Machine-readable checks for the installed shared Connector integration."""

from __future__ import annotations

import asyncio
import json
import os
import stat
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any

from meshcall import Mesh, MeshCallError, Router
from pydantic import BaseModel, ConfigDict

from connector.management.service import RENDEZVOUS_VERSION, rendezvous_path
from connector.mesh.hub import SHARED_SERVICE
from connector.runtimes.providers import default_runtime_providers


class Anything(BaseModel):
    model_config = ConfigDict(extra="allow")


async def _call(mesh: Mesh, method: str) -> dict[str, Any]:
    result = await mesh.call_unary(SHARED_SERVICE, method, Anything(), Anything, timeout=5)
    return result.model_dump()


async def _probe(path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    document = json.loads(path.read_text(encoding="utf-8"))
    if document.get("version") != RENDEZVOUS_VERSION or not isinstance(document.get("key"), str):
        raise ValueError("Shared service rendezvous is not a supported version")
    endpoint = document.get("endpoint")
    if not isinstance(endpoint, dict):
        raise TypeError("Shared service rendezvous has no endpoint")
    router = (
        Router(unix_path=endpoint["unixPath"])
        if isinstance(endpoint.get("unixPath"), str)
        else Router(pipe=endpoint["pipe"])
    )
    async with Mesh(router, token=document["key"]) as mesh:
        state = await _call(mesh, "getState")
        identity = await _call(mesh, "getIdentity")
    return state, identity


async def diagnose(path: Path | None = None, expected_runtimes: list[str] | None = None) -> dict[str, Any]:
    result: dict[str, Any] = {"ok": False, "checks": {}, "errors": []}
    checks = result["checks"]
    checks["runtimeTypes"] = [
        provider.runtime_type for provider in default_runtime_providers()
    ]
    missing = sorted(set(expected_runtimes or []) - set(checks["runtimeTypes"]))
    if missing:
        result["errors"].append("Missing runtime extensions: " + ", ".join(missing))
    checks["versions"] = {}
    for package in ("anywhere-cli", "pi-agents-anywhere"):
        try:
            checks["versions"][package] = version(package)
        except PackageNotFoundError:
            pass
    rendezvous = Path(path) if path is not None else rendezvous_path()
    checks["rendezvous"] = str(rendezvous)
    try:
        info = rendezvous.lstat()
        checks["privateRendezvous"] = (
            stat.S_ISREG(info.st_mode)
            and info.st_mode & 0o077 == 0
            and info.st_uid == os.getuid()
        )
        if not checks["privateRendezvous"]:
            result["errors"].append(
                "Shared service rendezvous must be a private regular file owned by the current user"
            )
            return result
        async with asyncio.timeout(15):
            state, identity = await _probe(rendezvous)
        checks["state"] = state
        checks["identity"] = identity
        if not state.get("running") or state.get("authFailed"):
            result["errors"].append(
                "Connector is stopped or requires credential recovery"
            )
    except (
        OSError,
        ValueError,
        RuntimeError,
        TimeoutError,
        MeshCallError,
    ) as exc:
        result["errors"].append(str(exc))
    result["ok"] = not result["errors"]
    return result
