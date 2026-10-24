"""Commands supported by AA's Codex runtime, including transcript-only side questions."""

from connector.runtime_protocol import RuntimeCommand

IDLE = ("idle", "error")
ALIASES = {"compact-thread": "compact"}


def list_codex_commands(
    external_session_id: str | None,
    client_available: bool,
    query: str | None = None,
    limit: int = 50,
    *,
    status: str = "idle",
    source_availability: str | None = None,
) -> tuple[RuntimeCommand, ...]:
    reason = None
    if not client_available:
        reason = "codex_unavailable"
    elif not external_session_id:
        reason = "session_unloaded"
    elif source_availability in {"archived", "unavailable", "deleted", "missing"}:
        reason = f"session_{source_availability}"
    side_reason = reason
    if side_reason is None and status not in (
        *IDLE,
        "running",
        "waiting",
        "waiting_approval",
        "blocked",
    ):
        side_reason = f"session_{status}"
    if reason is None and status not in IDLE:
        reason = f"session_{status}"
    command = RuntimeCommand(
        id="compact",
        title="Compact",
        description="Compact the session context.",
        aliases=tuple(ALIASES),
        enabled=reason is None,
        disabled_reason=reason,
        metadata={
            "ui": {
                "kind": "execute",
                "allowedStatuses": list(IDLE),
                "acceptsMultiline": False,
            }
        },
    )
    side_command = RuntimeCommand(
        id="btw",
        title="By the way",
        description="从当前会话记录回答旁支问题，不打断主任务。",
        accepts_args=True,
        enabled=side_reason is None,
        disabled_reason=side_reason,
        metadata={
            "ui": {
                "kind": "execute",
                "argumentHint": "问题（留空则重开上次问答）",
                "allowedStatuses": [
                    *IDLE,
                    "running",
                    "waiting",
                    "waiting_approval",
                    "blocked",
                ],
                "acceptsMultiline": True,
            }
        },
    )
    needle = (query or "").strip().casefold()
    return tuple(
        c
        for c in (side_command, command)
        if not needle
        or needle
        in " ".join((c.id, c.title, c.description or "", *c.aliases)).casefold()
    )[: max(0, min(limit, 1000))]
