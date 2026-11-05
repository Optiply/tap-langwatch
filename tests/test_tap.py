"""Credential-free tests: every HTTP call goes through a fake transport."""

from __future__ import annotations

import io
import json
from contextlib import redirect_stdout
from urllib.parse import urlparse

import pytest
import requests
from hotglue_etl_exceptions import InvalidCredentialsError
from hotglue_singer_sdk.exceptions import FatalAPIError

from tap_langwatch.tap import TapLangWatch

CONFIG = {"api_key": "sk-lw-test", "start_date": "2024-01-01T00:00:00Z"}
SEARCH = "/api/v1/traces/search"
ANNOTATIONS = "/api/v1/annotations"
START_MS = 1704067200000  # 2024-01-01T00:00:00Z


class FakeAPI:
    """Routes requests by path to queued responses and records every call."""

    def __init__(self) -> None:
        self.routes: dict[str, list] = {}
        self.calls: list[requests.PreparedRequest] = []
        self.sleeps: list[float] = []

    def add(self, path: str, *responses) -> None:
        self.routes[path] = list(responses)

    def send(self, adapter, request, **kwargs):
        self.calls.append(request)
        path = urlparse(request.url).path
        queue = self.routes.get(path)
        if not queue:
            raise AssertionError(f"Unexpected request {request.method} {request.url}")
        entry = queue.pop(0) if len(queue) > 1 else queue[0]
        status, body, headers = entry(request) if callable(entry) else entry
        response = requests.Response()
        response.status_code = status
        response._content = body if isinstance(body, bytes) else json.dumps(body).encode()
        response.headers.update({"Content-Type": "application/json", **headers})
        response.url = request.url
        response.request = request
        response.reason = "fake"
        return response

    def calls_to(self, path: str) -> list[requests.PreparedRequest]:
        return [c for c in self.calls if urlparse(c.url).path == path]


def body_of(request: requests.PreparedRequest) -> dict:
    return json.loads(request.body)


def trace(trace_id: str, updated_at: int, spans: list | None = None, evaluations: list | None = None) -> dict:
    return {
        "trace_id": trace_id,
        "project_id": "proj_1",
        "metadata": {"user_id": "u_1", "thread_id": "t_1", "customer_id": "c_1", "labels": ["x"], "custom": "y"},
        "timestamps": {"started_at": updated_at - 1000, "inserted_at": updated_at - 500, "updated_at": updated_at},
        "input": {"value": "hi"},
        "output": {"value": "hello"},
        "metrics": {"total_cost": 0.001, "prompt_tokens": 10, "completion_tokens": 5},
        "error": None,
        "events": [],
        "spans": spans if spans is not None else [],
        "evaluations": evaluations if evaluations is not None else [],
        "platformUrl": f"https://app.langwatch.ai/p/{trace_id}",
    }


def span(trace_id: str, span_id: str, **extra) -> dict:
    return {
        "span_id": span_id,
        "trace_id": trace_id,
        "parent_id": None,
        "type": "tool",
        "name": "lookup",
        "input": {"type": "json", "value": {"q": 1}},
        "output": {"type": "text", "value": "ok"},
        "timestamps": {"started_at": 1, "finished_at": 2},
        **extra,
    }


def evaluation(evaluation_id: str, **extra) -> dict:
    return {
        "evaluation_id": evaluation_id,
        "evaluator_id": "ev_1",
        "name": "Faithfulness",
        "status": "processed",
        "passed": True,
        "score": 0.9,
        "timestamps": {"finished_at": 3},
        **extra,
    }


def page(traces: list[dict], scroll_id: str | None = None, **pagination) -> tuple:
    body = {"traces": traces, "pagination": {"totalHits": len(traces), **pagination}}
    if scroll_id:
        body["pagination"]["scrollId"] = scroll_id
    return (200, body, {})


@pytest.fixture
def api(monkeypatch):
    fake = FakeAPI()
    monkeypatch.setattr(requests.adapters.HTTPAdapter, "send", lambda adapter, request, **kw: fake.send(adapter, request, **kw))
    monkeypatch.setattr("time.sleep", fake.sleeps.append)
    return fake


def make_tap(streams: set[str], state: dict | None = None, config: dict | None = None) -> TapLangWatch:
    cfg = config or CONFIG
    catalog = TapLangWatch(config=cfg).catalog_dict
    for entry in catalog["streams"]:
        for md in entry["metadata"]:
            if md["breadcrumb"] == []:
                md["metadata"]["selected"] = entry["tap_stream_id"] in streams
    return TapLangWatch(config=cfg, catalog=catalog, state=state or {})


def run_tap(streams: set[str], state: dict | None = None, config: dict | None = None) -> list[dict]:
    out = io.StringIO()
    with redirect_stdout(out):
        make_tap(streams, state, config).sync_all()
    return [json.loads(line) for line in out.getvalue().splitlines() if line.strip()]


def records(messages: list[dict], stream: str) -> list[dict]:
    return [m["record"] for m in messages if m["type"] == "RECORD" and m["stream"] == stream]


def last_state(messages: list[dict]) -> dict:
    return [m for m in messages if m["type"] == "STATE"][-1]["value"]


# Discovery


def test_discovery_exposes_every_stream_with_keys_and_replication():
    catalog = TapLangWatch(config=CONFIG).catalog_dict
    by_name = {s["tap_stream_id"]: s for s in catalog["streams"]}
    assert set(by_name) == {"traces", "spans", "evaluations", "annotations"}
    assert by_name["traces"]["key_properties"] == ["trace_id"]
    assert by_name["traces"]["replication_key"] == "updated_at"
    assert by_name["spans"]["key_properties"] == ["trace_id", "span_id"]
    assert by_name["evaluations"]["key_properties"] == ["trace_id", "evaluation_id"]
    assert by_name["annotations"]["key_properties"] == ["id"]
    for name, stream in by_name.items():
        for key in stream["key_properties"] + ([stream["replication_key"]] if stream.get("replication_key") else []):
            assert key in stream["schema"]["properties"], (name, key)


# Traces: request shape, pagination, state


def test_traces_first_run_requests_updated_window_from_start_date(api):
    api.add(SEARCH, page([trace("t1", START_MS + 5000)]))
    messages = run_tap({"traces"})
    body = body_of(api.calls_to(SEARCH)[0])
    assert body["startDate"] == START_MS
    assert body["dateField"] == "updated"
    assert body["format"] == "json"
    assert body["includeSpans"] is False
    assert body["pageSize"] == 200
    assert "scrollId" not in body
    assert body["endDate"] >= START_MS
    assert api.calls_to(SEARCH)[0].headers["X-Auth-Token"] == "sk-lw-test"
    (row,) = records(messages, "traces")
    assert row["updated_at"] == "2024-01-01T00:00:05.000Z"
    assert "spans" not in row and "evaluations" not in row
    assert json.loads(row["metadata"])["custom"] == "y"
    assert json.loads(row["timestamps"])["updated_at"] == START_MS + 5000
    assert row["error"] is None
    assert last_state(messages)["bookmarks"]["traces"]["replication_key_value"] == "2024-01-01T00:00:05.000Z"


def test_nested_fields_are_json_text_not_python_repr(api):
    """Hotglue's CSV stage writes str(value); only a JSON string survives it for BigQuery."""
    row = trace("t1", START_MS + 1000, spans=[span("t1", "s1", error={"has_error": True, "message": "it's \"quoted\""})], evaluations=[evaluation("e1", inputs={"q": None})])
    row["metadata"]["labels"] = ["a", "ü"]
    api.add(SEARCH, page([row]))
    messages = run_tap({"traces", "spans", "evaluations"})
    (tr,) = records(messages, "traces")
    (sp,) = records(messages, "spans")
    (ev,) = records(messages, "evaluations")
    for value in (tr["metadata"], tr["metrics"], tr["input"], sp["error"], sp["timestamps"], ev["inputs"], ev["timestamps"]):
        assert isinstance(value, str) and "'" not in value.replace("it's", "")
        json.loads(value)
    assert json.loads(tr["metadata"])["labels"] == ["a", "ü"]
    assert json.loads(sp["error"]) == {"has_error": True, "message": "it's \"quoted\""}
    assert json.loads(ev["inputs"]) == {"q": None}


def test_traces_follow_scroll_until_absent_and_bookmark_is_max_delivered(api):
    api.add(
        SEARCH,
        page([trace("t1", START_MS + 9000), trace("t2", START_MS + 1000)], scroll_id="s1"),
        page([trace("t3", START_MS + 4000)], scroll_id="s2"),
        page([trace("t4", START_MS + 2000)]),
    )
    messages = run_tap({"traces"})
    calls = api.calls_to(SEARCH)
    assert [body_of(c).get("scrollId") for c in calls] == [None, "s1", "s2"]
    assert [body_of(c)["startDate"] for c in calls] == [START_MS] * 3
    assert [r["trace_id"] for r in records(messages, "traces")] == ["t1", "t2", "t3", "t4"]
    assert last_state(messages)["bookmarks"]["traces"]["replication_key_value"] == "2024-01-01T00:00:09.000Z"


def test_traces_empty_page_with_scroll_id_ends_the_listing(api):
    api.add(SEARCH, page([trace("t1", START_MS + 1000)], scroll_id="s1"), page([], scroll_id="s2"))
    messages = run_tap({"traces"})
    assert len(api.calls_to(SEARCH)) == 2
    assert len(records(messages, "traces")) == 1


def test_traces_repeated_scroll_id_fails(api):
    api.add(SEARCH, page([trace("t1", START_MS + 1000)], scroll_id="same"), page([trace("t2", START_MS + 2000)], scroll_id="same"))
    with pytest.raises(RuntimeError, match="already visited scrollId"):
        run_tap({"traces"})


def test_traces_empty_window_keeps_bookmark(api):
    api.add(SEARCH, page([]))
    state = {"bookmarks": {"traces": {"replication_key": "updated_at", "replication_key_value": "2024-03-01T00:00:00.000Z"}}}
    messages = run_tap({"traces"}, state=state)
    assert body_of(api.calls_to(SEARCH)[0])["startDate"] == 1709251200000
    assert last_state(messages)["bookmarks"]["traces"]["replication_key_value"] == "2024-03-01T00:00:00.000Z"


def test_traces_second_run_resumes_from_bookmark_inclusive(api):
    api.add(SEARCH, page([trace("t1", START_MS + 5000)]))
    first = run_tap({"traces"})
    api.add(SEARCH, page([trace("t1", START_MS + 5000), trace("t2", START_MS + 7000)]))
    second = run_tap({"traces"}, state=last_state(first))
    assert body_of(api.calls_to(SEARCH)[1])["startDate"] == START_MS + 5000
    assert [r["trace_id"] for r in records(second, "traces")] == ["t1", "t2"]
    assert last_state(second)["bookmarks"]["traces"]["replication_key_value"] == "2024-01-01T00:00:07.000Z"


def test_traces_state_does_not_advance_when_a_later_page_fails(api):
    api.add(SEARCH, page([trace("t1", START_MS + 1000)], scroll_id="s1"), (500, {"error": "boom"}, {}))
    out = io.StringIO()
    with redirect_stdout(out), pytest.raises(Exception):
        make_tap({"traces"}).sync_all()
    states = [json.loads(l)["value"] for l in out.getvalue().splitlines() if '"STATE"' in l]
    assert all("replication_key_value" not in s["bookmarks"].get("traces", {}) for s in states)


def test_traces_without_updated_at_fail(api):
    row = trace("t1", START_MS + 1000)
    row["timestamps"].pop("updated_at")
    api.add(SEARCH, page([row]))
    with pytest.raises(FatalAPIError, match="timestamps.updated_at"):
        run_tap({"traces"})


def test_traces_malformed_body_fails(api):
    api.add(SEARCH, (200, {"nope": []}, {}))
    with pytest.raises(FatalAPIError, match="Malformed"):
        run_tap({"traces"})


def test_page_size_and_api_url_config(api):
    api.add(SEARCH, page([]))
    run_tap({"traces"}, config={**CONFIG, "page_size": 50, "api_url": "https://langwatch.example.com/"})
    (call,) = api.calls_to(SEARCH)
    assert call.url == "https://langwatch.example.com/api/v1/traces/search"
    assert body_of(call)["pageSize"] == 50


# Spans and evaluations


def test_spans_and_evaluations_flatten_from_traces_without_extra_requests(api):
    api.add(
        SEARCH,
        page(
            [
                trace("t1", START_MS + 1000, spans=[span("t1", "s1"), span("t1", "s2", parent_id="s1", type="llm", model="gpt")], evaluations=[evaluation("e1")]),
                trace("t2", START_MS + 2000, spans=[span("t2", "s1")], evaluations=[]),
            ]
        ),
    )
    messages = run_tap({"traces", "spans", "evaluations"})
    assert len(api.calls_to(SEARCH)) == 1
    body = body_of(api.calls_to(SEARCH)[0])
    assert body["includeSpans"] is True and body["pageSize"] == 10
    spans = records(messages, "spans")
    assert [(s["trace_id"], s["span_id"]) for s in spans] == [("t1", "s1"), ("t1", "s2"), ("t2", "s1")]
    assert spans[1]["model"] == "gpt" and spans[1]["parent_id"] == "s1"
    assert "input" not in spans[0] and "output" not in spans[0]
    (ev,) = records(messages, "evaluations")
    assert ev["trace_id"] == "t1" and ev["evaluation_id"] == "e1" and ev["score"] == 0.9
    assert "spans" not in records(messages, "traces")[0]
    bookmarks = last_state(messages)["bookmarks"]
    assert "partitions" not in bookmarks.get("spans", {}) and "partitions" not in bookmarks.get("evaluations", {})


def test_children_selected_alone_still_drive_the_search_and_keep_parent_progress(api):
    api.add(SEARCH, page([trace("t1", START_MS + 1000, spans=[span("t1", "s1")])]))
    messages = run_tap({"spans"})
    assert body_of(api.calls_to(SEARCH)[0])["includeSpans"] is True
    assert records(messages, "traces") == []
    assert len(records(messages, "spans")) == 1


def test_span_without_id_fails(api):
    bad = span("t1", "s1")
    bad.pop("span_id")
    api.add(SEARCH, page([trace("t1", START_MS + 1000, spans=[bad])]))
    with pytest.raises(FatalAPIError, match="without span_id"):
        run_tap({"spans"})


def test_span_content_is_pruned_but_tool_and_llm_facts_stay(api):
    params = {
        "langwatch": {"input": "whole history", "output": "..."},
        "llm": {"provider": "openai", "finish_reason": "stop", "input_messages": ["secret"], "system": "prompt", "tools": [{}]},
        "gen_ai": {"operation": "chat", "conversation": "secret", "input": "secret", "output": "secret", "usage": {"input_tokens": 1}},
        "tool": {"name": "execute_secure_sql", "description": "runs sql"},
        "metadata": {"a": 1},
        "input": "secret",
        "output": "secret",
        "unknown_key": "dropped",
    }
    api.add(SEARCH, page([trace("t1", START_MS + 1000, spans=[span("t1", "s1", params=params), span("t1", "s2", params=None)])]))
    (slim, no_params) = records(run_tap({"spans"}), "spans")
    assert json.loads(slim["params"]) == {
        "llm": {"provider": "openai", "finish_reason": "stop"},
        "gen_ai": {"operation": "chat", "usage": {"input_tokens": 1}},
        "tool": {"name": "execute_secure_sql", "description": "runs sql"},
        "metadata": {"a": 1},
    }
    assert "secret" not in json.dumps(slim)
    assert no_params["params"] is None and no_params["type"] == "tool"


def test_child_context_in_logs_carries_only_the_trace_id(api, caplog):
    """The SDK logs each child context in its METRIC lines; span content must stay out of it."""
    api.add(SEARCH, page([trace("t1", START_MS + 1000, spans=[span("t1", "s1", output={"type": "text", "value": "SECRET-CONVERSATION"})])]))
    with caplog.at_level("INFO"):
        messages = run_tap({"spans"})
    assert "output" not in records(messages, "spans")[0]
    assert "SECRET-CONVERSATION" not in caplog.text
    assert "'context': {'trace_id': 't1'}" in caplog.text


# Annotations


def test_annotations_full_table(api):
    rows = [
        {"id": "a1", "projectId": "p", "traceId": "t1", "userId": "u", "email": "e@x", "comment": "good", "isThumbsUp": True,
         "scoreOptions": {"quality": {"value": "5"}}, "expectedOutput": None, "anchorKind": None, "anchorId": None,
         "anchorPath": None, "createdAt": "2024-02-01T00:00:00.000Z", "updatedAt": "2024-02-02T00:00:00.000Z"},
    ]
    api.add(ANNOTATIONS, (200, {"data": rows}, {}))
    messages = run_tap({"annotations"})
    (call,) = api.calls_to(ANNOTATIONS)
    assert call.method == "GET" and call.headers["X-Auth-Token"] == "sk-lw-test"
    assert records(messages, "annotations") == [{**rows[0], "scoreOptions": '{"quality":{"value":"5"}}'}]
    assert "replication_key_value" not in last_state(messages)["bookmarks"].get("annotations", {})


def test_annotations_malformed_body_fails(api):
    api.add(ANNOTATIONS, (200, {"data": "x"}, {}))
    with pytest.raises(FatalAPIError, match="Malformed"):
        run_tap({"annotations"})


# HTTP errors


def test_401_raises_invalid_credentials_without_retry(api):
    api.add(SEARCH, (401, {"error": "Unauthorized"}, {}))
    with pytest.raises(InvalidCredentialsError):
        run_tap({"traces"})
    assert len(api.calls_to(SEARCH)) == 1


@pytest.mark.parametrize("status", [400, 403, 422])
def test_permanent_4xx_is_fatal(api, status):
    api.add(SEARCH, (status, {"error": "no"}, {}))
    with pytest.raises(FatalAPIError):
        run_tap({"traces"})
    assert len(api.calls_to(SEARCH)) == 1


def test_429_waits_retry_after_then_retries(api):
    api.add(SEARCH, (429, {"error": "slow"}, {"Retry-After": "7"}), page([]))
    run_tap({"traces"})
    assert len(api.calls_to(SEARCH)) == 2
    assert 7.0 in api.sleeps


def test_429_without_retry_after_retries(api):
    api.add(SEARCH, (429, {"error": "slow"}, {}), page([]))
    run_tap({"traces"})
    assert len(api.calls_to(SEARCH)) == 2


def test_5xx_retries_are_bounded(api):
    api.add(SEARCH, (503, {"error": "down"}, {}))
    with pytest.raises(Exception):
        run_tap({"traces"})
    assert len(api.calls_to(SEARCH)) == 5


def test_non_json_response_fails(api):
    api.add(SEARCH, (200, b"<html>", {}))
    with pytest.raises(FatalAPIError, match="Non-JSON"):
        run_tap({"traces"})
