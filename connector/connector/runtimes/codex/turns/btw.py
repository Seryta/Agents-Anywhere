"""Connector-only side questions: independent of native turns and model history."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable, Mapping
from typing import Any
from uuid import uuid4

from connector.logging import logger
from connector.runtime_protocol import (
    RuntimeCommandResult,
    RuntimeInvalidRequestError,
    RuntimeTimelineItem,
    timeline_content_hash,
)
from connector.runtime_protocol.host import RuntimeHostClient
from connector.runtimes.codex.sdk.runtime_client import CodexRuntimeClient
from connector.runtimes.codex.sdk.side_question import SideQuestionError


def transcript_from_thread(thread: Mapping[str, Any]) -> str:
    items = [item for turn in thread.get("turns", []) for item in turn.get("items", [])]
    items = items or thread.get("items", [])
    sections = []
    for item in items[-30:]:
        kind = item.get("type")
        if kind == "userMessage":
            text = "\n".join(
                p.get("text", "")
                for p in item.get("content", item.get("input", []))
                if p.get("type") == "text"
            )
            sections.append(f"User: {text}")
        elif kind == "agentMessage":
            sections.append(f"Assistant: {item.get('text', '')}")
        elif kind in {
            "commandExecution",
            "mcpToolCall",
            "dynamicToolCall",
            "fileChange",
        }:
            text = json_text(
                item.get(
                    "aggregatedOutput", item.get("result", item.get("changes", ""))
                )
            )[:800]
            sections.append(f"Tool {kind}: {text}")
        elif kind == "contextCompaction" and item.get("text"):
            sections.append(f"Summary: {item['text']}")
    transcript = "\n\n".join(sections)
    return (
        transcript
        if len(transcript) <= 16000
        else "...[earlier session context omitted]\n" + transcript[-16000:]
    )


def json_text(value: Any) -> str:
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False)


class CodexSideQuestions:
    def __init__(
        self,
        host: RuntimeHostClient,
        client: CodexRuntimeClient | None,
        ensure_started: Callable[[], Awaitable[None]],
        selected_model: Callable[[str], Awaitable[str | None]],
    ) -> None:
        self.host, self.client = host, client
        self.ensure_started, self.selected_model = ensure_started, selected_model
        self.owner = str(uuid4())
        self.tasks: dict[str, asyncio.Task[None]] = {}
        self.pending: dict[str, tuple[str, dict[str, Any]]] = {}
        self.lock = asyncio.Lock()
        self.stopping = False

    async def records(self, thread_id: str) -> list[dict[str, Any]]:
        value = await self.host.sync_state_read(f"codex/btw/{thread_id}")
        records = [dict(item) for item in (value or {}).get("items", [])]
        for item in records:
            if item["status"] == "running" and item.get("owner") != self.owner:
                item.update(
                    status="failed", answer="共享服务已停止，旁支问答中断。", revision=2
                )
        return records

    async def items(
        self, session_id: str, thread_id: str
    ) -> tuple[RuntimeTimelineItem, ...]:
        return tuple(
            self.project(session_id, thread_id, record)
            for record in await self.records(thread_id)
        )

    def project(
        self, session_id: str, thread_id: str, record: dict[str, Any]
    ) -> RuntimeTimelineItem:
        status = record["status"]
        content = {
            "kind": "error" if status == "failed" else "system",
            "text": f"/btw {record['question']}\n\n{record.get('answer', '正在从会话记录回答…')}",
        }
        return RuntimeTimelineItem(
            id=f"codex:btw:{thread_id}:{record['id']}",
            session_id=session_id,
            type="system",
            status=status,
            role="system",
            order_seq=0,
            content=content,
            revision=record["revision"],
            content_hash=timeline_content_hash(
                item_type="system", status=status, role="system", content=content
            ),
            source={
                "runtime": "codex",
                "threadId": thread_id,
                "rawType": "codex.btw",
                "itemId": record["id"],
            },
        )

    async def save(
        self, session_id: str, thread_id: str, record: dict[str, Any]
    ) -> None:
        # Persist first. A failed live upsert is repaired by the next history sync.
        async with self.lock:
            records = await self.records(thread_id)
            records = [
                record if item["id"] == record["id"] else item for item in records
            ]
            if not any(item["id"] == record["id"] for item in records):
                records.append(dict(record))
            await self.host.sync_state_write(
                f"codex/btw/{thread_id}", {"items": records}
            )
        try:
            await self.host.timeline_item_upsert(
                self.project(session_id, thread_id, record)
            )
        except Exception:  # noqa: BLE001 - durable history is the delivery fallback
            logger.warning("codex /btw live delivery failed; history sync will retry")

    async def execute(
        self, session_id: str, thread_id: str, question: str
    ) -> RuntimeCommandResult:
        if self.stopping:
            return RuntimeCommandResult(
                command="btw",
                ok=False,
                code="command_unavailable",
                message="Codex is stopping.",
            )
        if not question:
            previous = next(
                (
                    item
                    for item in reversed(await self.records(thread_id))
                    if item["status"] != "running"
                ),
                None,
            )
            if previous is None:
                return RuntimeCommandResult(
                    command="btw",
                    ok=False,
                    code="invalid_command",
                    message="还没有可重开的旁支问答。用法：/btw 你的问题",
                )
            record = {**previous, "id": str(uuid4()), "revision": 1}
            await self.save(session_id, thread_id, record)
            return RuntimeCommandResult(
                command="btw",
                result={"executionState": "completed", "commandId": record["id"]},
            )
        await self.ensure_started()
        if self.stopping:
            return RuntimeCommandResult(
                command="btw",
                ok=False,
                code="command_unavailable",
                message="Codex is stopping.",
            )
        if thread_id in self.tasks:
            return RuntimeCommandResult(
                command="btw",
                ok=False,
                code="command_unavailable",
                message="上一条旁支问答仍在运行。",
            )
        record = {
            "id": str(uuid4()),
            "question": question,
            "status": "running",
            "owner": self.owner,
            "revision": 1,
        }
        # Reserve before persistence awaits, so simultaneous commands cannot both start.
        ready = asyncio.Event()
        task = asyncio.create_task(self.run(session_id, thread_id, record, ready))
        self.tasks[thread_id] = task
        self.pending[thread_id] = (session_id, record)
        try:
            await self.save(session_id, thread_id, record)
        except BaseException:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            self.tasks.pop(thread_id, None)
            self.pending.pop(thread_id, None)
            raise
        ready.set()
        return RuntimeCommandResult(
            command="btw",
            result={"executionState": "accepted", "commandId": record["id"]},
        )

    async def run(
        self,
        session_id: str,
        thread_id: str,
        record: dict[str, Any],
        ready: asyncio.Event,
    ) -> None:
        try:
            await ready.wait()
            async with asyncio.timeout(120):
                list_turns = getattr(self.client, "list_thread_turns", None)
                result = await self.client.read_thread(
                    thread_id, include_turns=not callable(list_turns)
                )
                thread = result.thread
                if not isinstance(thread, Mapping):
                    thread = thread.model_dump(by_alias=True)
                # Fetch complete paginated history when the SDK exposes it; never resume a busy writer.
                if callable(list_turns):
                    try:
                        turns = await list_turns(thread_id)
                    except RuntimeInvalidRequestError:
                        result = await self.client.read_thread(
                            thread_id, include_turns=True
                        )
                        thread = (
                            result.thread
                            if isinstance(result.thread, Mapping)
                            else result.thread.model_dump(by_alias=True)
                        )
                    else:
                        thread = {**thread, "turns": turns.turns}
                answer = await self.client.answer_side_question(
                    record["question"],
                    transcript_from_thread(thread),
                    cwd=thread.get("cwd"),
                    model=await self.selected_model(session_id) or thread.get("model"),
                    provider_id=thread.get("modelProvider"),
                )
                record.update(answer=answer, status="done", revision=2)
        except asyncio.CancelledError:
            record.update(answer="旁支问答已中断。", status="failed", revision=2)
        except TimeoutError:
            record.update(answer="旁支问答超时，请重试。", status="failed", revision=2)
        except SideQuestionError as exc:
            record.update(answer=str(exc), status="failed", revision=2)
        except Exception:  # noqa: BLE001 - do not persist provider errors that may contain credentials
            record.update(
                answer="旁支问答失败，请检查 Codex 登录和模型配置。",
                status="failed",
                revision=2,
            )
        finally:
            try:
                await self.save(session_id, thread_id, record)
            except Exception:  # noqa: BLE001 - next runtime settles orphaned running records
                logger.warning("codex /btw outcome persistence failed")
            if self.tasks.get(thread_id) is asyncio.current_task():
                self.tasks.pop(thread_id, None)
                self.pending.pop(thread_id, None)

    async def stop(self) -> None:
        self.stopping = True
        tasks = tuple(self.tasks.values())
        pending = tuple(self.pending.items())
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        # A task cancelled before its first poll never enters run()'s finally.
        for thread_id, (session_id, record) in pending:
            if record["status"] == "running":
                record.update(answer="旁支问答已中断。", status="failed", revision=2)
                try:
                    await self.save(session_id, thread_id, record)
                except Exception:  # noqa: BLE001 - persistence must not block native shutdown
                    logger.warning("codex /btw shutdown persistence failed")
        self.tasks.clear()
        self.pending.clear()
