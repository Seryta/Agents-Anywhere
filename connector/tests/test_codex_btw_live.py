"""Real bundled Codex + deterministic local Responses server; no paid calls."""

import asyncio
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from test_codex_runtime import FakeHost, _config

from connector.runtime_protocol import RuntimeConfig
from connector.runtimes.codex.runtime import CodexRuntime
from connector.runtimes.codex.sdk.client import sdk_client_from_config


async def wait_for(predicate):
    async with asyncio.timeout(30):
        while not predicate():
            await asyncio.sleep(0.02)


def test_real_codex_btw_answers_while_native_main_turn_remains_running(tmp_path: Path):
    requests = []
    release_main = threading.Event()
    main_started = threading.Event()

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *args):
            pass

        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            requests.append(body)
            side = body.get("tool_choice") == "none"
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Connection", "close")
            self.end_headers()
            text = "暗号是四十二。" if side else "主任务正常完成。"
            message = {
                "type": "message",
                "id": "msg-side" if side else "msg-main",
                "role": "assistant",
                "status": "completed",
                "content": [{"type": "output_text", "text": text, "annotations": []}],
            }

            def emit(event):
                self.wfile.write(
                    ("data: " + json.dumps(event, ensure_ascii=False) + "\n\n").encode()
                )
                self.wfile.flush()

            emit(
                {
                    "type": "response.created",
                    "response": {
                        "id": "resp-side" if side else "resp-main",
                        "status": "in_progress",
                        "output": [],
                    },
                }
            )
            if not side:
                main_started.set()
                if not release_main.wait(45):
                    return
            emit(
                {
                    "type": "response.output_item.added",
                    "output_index": 0,
                    "item": {**message, "status": "in_progress", "content": []},
                }
            )
            emit(
                {
                    "type": "response.output_text.delta",
                    "output_index": 0,
                    "content_index": 0,
                    "item_id": message["id"],
                    "delta": text,
                }
            )
            emit(
                {
                    "type": "response.output_item.done",
                    "output_index": 0,
                    "item": message,
                }
            )
            emit(
                {
                    "type": "response.completed",
                    "response": {
                        "id": "resp-side" if side else "resp-main",
                        "status": "completed",
                        "output": [message],
                        "usage": {
                            "input_tokens": 10,
                            "output_tokens": 5,
                            "total_tokens": 15,
                        },
                    },
                }
            )
            self.close_connection = True

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    async def check():
        home = tmp_path / "codex-home"
        home.mkdir()
        (home / "config.toml").write_text(
            'model = "gpt-6.1-sol"\n[features]\nshell_tool = false\n'
        )
        config = RuntimeConfig(
            runtime="codex",
            revision=_config().revision,
            values={
                "codexHome": str(home),
                "useSystemCodex": False,
                "modelGateway": {
                    "baseUrl": f"http://127.0.0.1:{server.server_port}/v1",
                    "apiKey": "test-only",
                },
                "environment": {"HOME": str(tmp_path)},
            },
        )
        client = sdk_client_from_config(config)
        host = FakeHost()
        runtime = CodexRuntime(config=config, host=host, client=client)
        try:
            result = await runtime.create_and_start_session(
                "live", "记住暗号是四十二，主任务还在进行中。", cwd=str(tmp_path)
            )
            assert result.ok
            await wait_for(main_started.is_set)
            state = runtime._session_states.get("live")
            native_id = state.external_session_id
            command = await runtime.execute_command(
                "live", "btw", native_id, "/btw What is the secret?"
            )
            assert command.ok and command.result["executionState"] == "accepted"
            await wait_for(
                lambda: any(
                    item.source.get("rawType") == "codex.btw"
                    and item.status in {"done", "failed"}
                    for item in host.timeline_item_upserts
                )
            )
            side = next(
                item
                for item in reversed(host.timeline_item_upserts)
                if item.source.get("rawType") == "codex.btw"
            )
            assert side.status == "done", side.content
            assert "暗号是四十二" in side.content["text"]
            assert runtime._session_states.get("live").status == "running"
            side_request = next(
                request for request in requests if request.get("tool_choice") == "none"
            )
            assert (
                side_request["tools"] == []
                and "Chinese" in side_request["instructions"]
            )
            assert "暗号是四十二" in str(side_request["input"])
            release_main.set()
            await wait_for(lambda: runtime._session_states.get("live").status == "idle")
            snapshot = await runtime.get_session_snapshot("live", native_id)
            assert sum(item.id == side.id for item in snapshot.items) == 1
            history = await client.read_thread(native_id, include_turns=True)
            native = (
                history.thread.model_dump(by_alias=True)
                if hasattr(history.thread, "model_dump")
                else dict(history.thread)
            )
            turns = await client.list_thread_turns(native_id)
            native["turns"] = turns.turns
            assert "What is the secret?" not in json.dumps(native, ensure_ascii=False)
            assert len(requests) == 2
        finally:
            release_main.set()
            await runtime.stop()

    try:
        asyncio.run(check())
    finally:
        release_main.set()
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
