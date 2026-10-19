"""Machine-readable checks for the installed shared Connector integration."""

from __future__ import annotations

import asyncio
import json
import os
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any

from connector.runtimes.providers import default_runtime_providers


async def diagnose(socket_path: Path, expected_runtimes: list[str]) -> dict[str, Any]:
    result: dict[str, Any] = {"ok": False, "checks": {}, "errors": []}
    checks = result["checks"]
    try:
        checks["runtimeTypes"] = [
            provider.runtime_type for provider in default_runtime_providers()
        ]
        missing = sorted(set(expected_runtimes) - set(checks["runtimeTypes"]))
        if missing:
            result["errors"].append("Missing runtime extensions: " + ", ".join(missing))
        checks["versions"] = {}
        for package in ("anywhere-cli", "pi-agents-anywhere"):
            try:
                checks["versions"][package] = version(package)
            except PackageNotFoundError:
                pass
        info = socket_path.lstat()
        checks["privateSocket"] = (
            info.st_mode & 0o077 == 0 and info.st_uid == os.getuid()
        )
        if not checks["privateSocket"]:
            result["errors"].append(
                "Local socket must be private and owned by the current user"
            )
            return result
        async with asyncio.timeout(5):
            reader, writer = await asyncio.open_unix_connection(socket_path)
            try:
                for index, (method, key) in enumerate(
                    (
                        ("connector.getState", "state"),
                        ("connector.getIdentity", "identity"),
                    )
                ):
                    writer.write(
                        json.dumps(
                            {"jsonrpc": "2.0", "id": index, "method": method}
                        ).encode()
                        + b"\n"
                    )
                    await writer.drain()
                    while True:
                        frame = json.loads(await reader.readline())
                        if frame.get("id") == index:
                            if "error" in frame:
                                raise RuntimeError("Local control request was rejected")
                            checks[key] = frame["result"]
                            break
            finally:
                writer.close()
                await writer.wait_closed()
        if checks["state"].get("protocolVersion") != 1:
            result["errors"].append("Unsupported local control protocol")
        if not checks["state"].get("running") or checks["state"].get("authFailed"):
            result["errors"].append(
                "Connector is stopped or requires credential recovery"
            )
    except (OSError, ValueError, RuntimeError, TimeoutError) as exc:
        result["errors"].append(str(exc))
    result["ok"] = not result["errors"]
    return result
