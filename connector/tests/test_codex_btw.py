"""Side answers arrive during the main turn and never become native user input."""

import asyncio
import base64
import json

import httpx
import pytest
from test_codex_commands import runtime_fixture
from test_codex_runtime import FakeCodexClient

from connector.runtime_protocol import RuntimeInvalidRequestError
from connector.runtimes.codex.runtime import CodexRuntime
from connector.runtimes.codex.sdk.side_question import answer_side_question
from connector.runtimes.codex.turns.btw import transcript_from_thread
from connector.runtimes.model_gateway import ModelGateway


class SideClient(FakeCodexClient):
    def __init__(self):
        super().__init__()
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.side_calls = []
        self.error = None

    async def answer_side_question(self, question, transcript, **kwargs):
        self.side_calls.append((question, transcript, kwargs))
        self.entered.set()
        await self.release.wait()
        if self.error:
            raise self.error
        return "暗号是四十二。"


async def settle(runtime):
    await asyncio.gather(*tuple(runtime._side_questions.tasks.values()))


@pytest.mark.parametrize(
    "status", ["idle", "running", "waiting", "waiting_approval", "blocked", "error"]
)
def test_btw_enabled_while_compact_needs_idle(status):
    async def check():
        runtime, _, _ = await runtime_fixture(status, SideClient())
        catalog = {c.id: c for c in await runtime.list_commands("sess_1", "thread_1")}
        assert catalog["btw"].enabled and catalog["btw"].accepts_args
        assert catalog["btw"].metadata["ui"]["acceptsMultiline"]
        assert catalog["compact"].enabled == (status in {"idle", "error"})
        assert [
            c.id for c in await runtime.list_commands("sess_1", "thread_1", "btw", 1)
        ] == ["btw"]

    asyncio.run(check())


@pytest.mark.parametrize("failure", [False, True])
def test_answer_arrives_before_main_turn_ends_and_survives_restart(failure):
    async def check():
        runtime, client, host = await runtime_fixture("running", SideClient())
        if failure:
            client.error = RuntimeError("provider echoed SECRET-CREDENTIAL")
        runtime._active_turn_ids["sess_1"] = "main"
        result = await runtime.execute_command(
            "sess_1", "btw", "thread_1", "/btw 暗号是什么？"
        )
        assert result.ok and result.result["executionState"] == "accepted"
        assert host.timeline_item_upserts[-1].status == "running"
        await client.entered.wait()
        assert (
            await runtime.execute_command("sess_1", "btw", "thread_1", "/btw 再问一次")
            != result
        )
        client.release.set()
        await settle(runtime)
        side = host.timeline_item_upserts[-1]
        assert side.id == host.timeline_item_upserts[0].id
        assert side.type == "system" and side.turn_id is None
        assert side.status == ("failed" if failure else "done")
        assert "SECRET" not in side.content["text"]
        assert (
            await runtime.get_session_state("sess_1", "thread_1")
        ).status == "running"
        assert runtime._active_turn_ids["sess_1"] == "main"
        assert not any(
            method
            in {
                "turn/start",
                "turn/steer",
                "turn/interrupt",
                "thread/resume",
                "thread/fork",
            }
            for method, _ in client.requests
        )
        before = len(client.side_calls)
        reopened = await runtime.execute_command("sess_1", "btw", "thread_1", "/btw")
        assert reopened.ok and reopened.result["executionState"] == "completed"
        assert len(client.side_calls) == before
        assert host.timeline_item_upserts[-1].id != side.id
        assert host.timeline_item_upserts[-1].content == side.content
        restored = CodexRuntime(config=runtime.config, host=host, client=client)
        snapshot = await restored.get_session_snapshot("sess_1", "thread_1")
        assert sum(item.id == side.id for item in snapshot.items) == 1
        assert not any("/btw" in str(params) for _, params in client.requests)
        await restored.stop()
        await runtime.stop()

    asyncio.run(check())


def test_shutdown_settles_side_question_without_interrupting_native_main():
    async def check():
        runtime, client, host = await runtime_fixture("running", SideClient())
        await runtime.execute_command("sess_1", "btw", "thread_1", "/btw 问题")
        await client.entered.wait()
        await runtime.stop()
        assert not runtime._side_questions.tasks
        assert host.timeline_item_upserts[-1].status == "failed"
        assert not any(method == "turn/interrupt" for method, _ in client.requests)
        assert "中断" in host.timeline_item_upserts[-1].content["text"]

    asyncio.run(check())


def test_lost_live_delivery_is_recovered_by_durable_history_sync():
    async def check():
        runtime, client, host = await runtime_fixture("running", SideClient())

        async def broken(item):
            raise ConnectionError("offline")

        host.timeline_item_upsert = broken
        await runtime.execute_command("sess_1", "btw", "thread_1", "/btw 问题")
        await client.entered.wait()
        client.release.set()
        await settle(runtime)
        prepared = await runtime.prepare_session_timeline_sync("sess_1", "thread_1")
        assert (
            len(
                [
                    i
                    for i in prepared.snapshot.items
                    if i.source.get("rawType") == "codex.btw"
                ]
            )
            == 1
        )
        assert prepared.snapshot.items[-1].status == "done"
        await prepared.commit()
        assert (
            await runtime.prepare_session_timeline_sync("sess_1", "thread_1")
        ).snapshot is None
        await runtime.stop()

    asyncio.run(check())


def test_reopen_without_history_and_mismatched_raw_never_call_model():
    async def check():
        runtime, client, _ = await runtime_fixture("running", SideClient())
        for raw in ("/btw", "/compact 问题"):
            result = await runtime.execute_command("sess_1", "btw", "thread_1", raw)
            assert not result.ok and result.code == "invalid_command"
        assert not client.requests and not client.side_calls

    asyncio.run(check())


def test_transcript_is_bounded_and_excludes_side_records_and_reasoning():
    thread = {
        "turns": [
            {
                "items": [
                    {"type": "reasoning", "text": "PRIVATE REASONING"},
                    {"type": "codex.btw", "text": "OLD SIDE ANSWER"},
                    {
                        "type": "userMessage",
                        "content": [{"type": "text", "text": "x" * 18000}],
                    },
                    {"type": "commandExecution", "aggregatedOutput": "t" * 2000},
                    {"type": "agentMessage", "text": "暗号是四十二"},
                ]
            }
        ]
    }
    transcript = transcript_from_thread(thread)
    assert "暗号是四十二" in transcript
    assert "PRIVATE" not in transcript and "OLD SIDE" not in transcript
    assert len(transcript) < 16100 and "t" * 801 not in transcript


class RpcClient:
    def __init__(self, config=None, auth=None):
        self.config = config or {"model": "example"}
        self.auth = auth or {}
        self.calls = []

    async def request(self, method, params, *, response_model):
        self.calls.append((method, params))
        return response_model.model_validate(
            {"config": self.config} if method == "config/read" else self.auth
        )


def sse(*events):
    return "".join("data: " + json.dumps(event) + "\n\n" for event in events)


@pytest.mark.parametrize("route", ["gateway", "env", "chatgpt", "api_key"])
def test_one_shot_uses_current_route_with_no_tools_or_history(route):
    async def check():
        payload = (
            base64.urlsafe_b64encode(
                json.dumps(
                    {
                        "https://api.openai.com/auth": {
                            "chatgpt_account_id": "test-account"
                        }
                    }
                ).encode()
            )
            .decode()
            .rstrip("=")
        )
        token = f"head.{payload}.signature" if route == "chatgpt" else "test-token"
        provider = {
            "base_url": "https://model.invalid/v1",
            "env_key": "TEST_MODEL_KEY",
            "query_params": {"version": "one"},
            "http_headers": {"X-Route": "test"},
        }
        native = RpcClient(
            {
                "model": "current-model",
                **(
                    {
                        "model_provider": "custom",
                        "model_providers": {"custom": provider},
                    }
                    if route == "env"
                    else {}
                ),
            },
            {
                "authMethod": "chatgpt" if route == "chatgpt" else "apikey",
                "authToken": token,
            },
        )
        captured = []

        async def respond(request):
            captured.append(request)
            body = json.loads(request.content)
            assert body["tools"] == [] and body["tool_choice"] == "none"
            assert body["store"] is False and body["stream"] is True
            assert body["model"] == "selected-model"
            assert "Chinese" in body["instructions"]
            assert "暗号" in str(body["input"]) and "transcript-secret" in str(
                body["input"]
            )
            assert "previous_response_id" not in body and "conversation" not in body
            return httpx.Response(
                200,
                text=sse(
                    {"type": "response.output_text.delta", "delta": "四十"},
                    {"type": "response.output_text.delta", "delta": "二。"},
                    {"type": "response.completed", "response": {"status": "completed"}},
                ),
            )

        answer = await answer_side_question(
            native,
            "暗号？",
            "transcript-secret",
            model="selected-model",
            gateway=ModelGateway("https://gateway.invalid/v1", "gateway-token")
            if route == "gateway"
            else None,
            environment={"TEST_MODEL_KEY": token},
            transport=httpx.MockTransport(respond),
        )
        assert answer == "四十二。" and len(captured) == 1
        request = captured[0]
        assert (
            request.headers["Authorization"]
            == f"Bearer {'gateway-token' if route == 'gateway' else token}"
        )
        if route == "chatgpt":
            assert str(request.url) == "https://chatgpt.com/backend-api/codex/responses"
            assert request.headers["ChatGPT-Account-Id"] == "test-account"
        elif route == "env":
            assert (
                request.url.host == "model.invalid"
                and request.url.params["version"] == "one"
            )
            assert request.headers["X-Route"] == "test"
        assert [method for method, _ in native.calls] == (
            ["config/read"]
            if route in {"env", "gateway"}
            else ["config/read", "getAuthStatus"]
        )

    asyncio.run(check())


@pytest.mark.parametrize(
    "events",
    [
        [],
        [{"type": "response.output_text.delta", "delta": "partial"}],
        [{"type": "response.failed", "response": {"error": "SECRET"}}],
    ],
)
def test_stream_without_successful_terminal_is_failure(events):
    async def check():
        transport = httpx.MockTransport(
            lambda request: httpx.Response(200, text=sse(*events))
        )
        with pytest.raises(RuntimeInvalidRequestError) as error:
            await answer_side_question(
                RpcClient(),
                "问题",
                "",
                gateway=ModelGateway("https://model.invalid", "test"),
                transport=transport,
            )
        assert "SECRET" not in str(error.value)

    asyncio.run(check())


def test_simultaneous_questions_only_start_one_model_request():
    async def check():
        runtime, client, _ = await runtime_fixture("running", SideClient())
        results = await asyncio.gather(
            *(
                runtime.execute_command(
                    "sess_1", "btw", "thread_1", f"/btw question {i}"
                )
                for i in range(2)
            )
        )
        assert sum(result.ok for result in results) == 1
        await client.entered.wait()
        assert len(client.side_calls) == 1
        client.release.set()
        await settle(runtime)
        await runtime.stop()

    asyncio.run(check())


def test_orphaned_running_record_becomes_interrupted_and_can_reopen():
    async def check():
        runtime, client, host = await runtime_fixture("running", SideClient())
        await runtime.execute_command("sess_1", "btw", "thread_1", "/btw question")
        await client.entered.wait()
        restored = CodexRuntime(config=runtime.config, host=host, client=client)
        items = await restored._side_questions.items("sess_1", "thread_1")
        assert len(items) == 1 and items[0].status == "failed"
        assert "中断" in items[0].content["text"]
        reopened = await restored.execute_command("sess_1", "btw", "thread_1", "/btw")
        assert reopened.ok and len(client.side_calls) == 1
        await runtime.stop()
        await restored.stop()

    asyncio.run(check())


@pytest.mark.parametrize("status", [401, 403, 429, 500])
def test_http_error_does_not_publish_provider_secrets(status):
    async def check():
        transport = httpx.MockTransport(
            lambda request: httpx.Response(status, text="provider echoed SECRET-TOKEN")
        )
        with pytest.raises(RuntimeInvalidRequestError) as error:
            await answer_side_question(
                RpcClient(),
                "question",
                "",
                gateway=ModelGateway("https://model.invalid", "secret"),
                transport=transport,
            )
        assert f"HTTP {status}" in str(error.value) and "SECRET" not in str(error.value)

    asyncio.run(check())


def test_stop_immediately_after_acceptance_settles_even_an_unstarted_task():
    async def check():
        runtime, _, host = await runtime_fixture("running", SideClient())
        command = await runtime.execute_command(
            "sess_1", "btw", "thread_1", "/btw question"
        )
        assert command.ok
        await runtime.stop()
        assert host.timeline_item_upserts[-1].status == "failed"
        stored = host.sync_states["codex/btw/thread_1"]["items"]
        assert len(stored) == 1 and stored[0]["status"] == "failed"
        assert not runtime._side_questions.pending

    asyncio.run(check())


def test_side_history_does_not_mask_missing_native_change_marker():
    async def check():
        runtime, client, _ = await runtime_fixture("running", SideClient())
        command = await runtime.execute_command(
            "sess_1", "btw", "thread_1", "/btw question"
        )
        assert command.ok
        await client.entered.wait()
        client.release.set()
        await settle(runtime)
        meta = await runtime._session_reader._session_meta_from_thread_ref(
            "thread_1", {"id": "thread_1"}, False, "available", "now"
        )
        assert meta.metadata["sync"]["marker"] is None
        assert meta.metadata["sync"]["requires_timeline_sync"] is True
        await runtime.stop()

    asyncio.run(check())
