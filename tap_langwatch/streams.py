"""LangWatch stream definitions: traces (search), their nested spans and evaluations, and annotations."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterable

import requests
from hotglue_singer_sdk.exceptions import FatalAPIError

from tap_langwatch.client import LangWatchStream, epoch_ms_to_iso

MAX_PAGE_SIZE = 1000  # server cap per page (projection DSL docs)
# Measured 2026-11-04: ~75 spans/trace, 15 MB per trace with spans; 50 traces + includeSpans
# answers 500 from LangWatch, 10 is ~150 MB / 11 s. Raise only after re-measuring.
SPAN_PAGE_SIZE = 10
# Span content kept beyond the always-kept top-level fields. Dropped: span `input`/`output`,
# `params.langwatch` (message history re-attached to every span), `params.llm.input_messages`
# / `system` / `tools`, `params.gen_ai` conversation/input/output/system_instructions.
# The conversation itself is in `traces.input`/`traces.output`.
SPAN_PARAMS_KEPT: dict[str, set[str] | None] = {
    "tool": None,  # None = whole sub-object
    "metadata": None,
    "event": None,
    "llm": {"provider", "finish_reason"},
    "gen_ai": {"operation", "request", "response", "usage"},
}


class TracesStream(LangWatchStream):
    """`POST /api/v1/traces/search` on the `updated` date axis, raw `json` format.

    One request window per run: bookmark (or `start_date`) .. sync start, paged with
    `scrollId`. The bookmark is the largest `updated_at` actually delivered, never the
    requested end: the scroll may cover less than `endDate` (`pagination.updatedThrough`),
    and a trace modified while paging lands in the next window. Both bounds are inclusive,
    so a trace updated exactly at the bookmark is delivered again; consumers deduplicate
    by `trace_id`.
    """

    name = "traces"
    path = "/api/v1/traces/search"
    rest_method = "POST"
    primary_keys = ["trace_id"]
    replication_key = "updated_at"
    is_sorted = False
    json_fields = ("metadata", "timestamps", "input", "output", "expected_output", "contexts", "metrics", "error", "events", "privacy")

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.sync_started_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        self._seen_scroll_ids: set[str] = set()
        #: Spans and evaluations of the trace being emitted. Children sync right after
        #: each parent record, so one slot suffices; they stay out of the child context
        #: because the SDK logs that context (customer conversations) in its metrics.
        self.nested: dict[str, list] = {}

    def get_replication_key_signpost(self, context: dict | None) -> None:
        return None

    def get_url_params(self, context: dict | None, next_page_token: Any) -> dict[str, Any]:
        return {}

    def prepare_request_payload(self, context: dict | None, next_page_token: Any) -> dict:
        start = self.get_starting_timestamp(context) if self.replication_key else None
        start = start or self.get_config_start_date()
        body: dict[str, Any] = {
            "startDate": int(start.timestamp() * 1000),
            "endDate": self.sync_started_ms,
            "dateField": "updated",
            "format": "json",
            "includeSpans": self._child_selected("spans"),
            "pageSize": min(self.config["page_size"], SPAN_PAGE_SIZE) if self._child_selected("spans") else self.config["page_size"],
        }
        if next_page_token:
            body["scrollId"] = next_page_token
        return body

    def _child_selected(self, name: str) -> bool:
        return any(child.name == name and child.selected for child in self.child_streams)

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        body = self.json_body(response)
        traces = body.get("traces")
        if not isinstance(traces, list):
            raise FatalAPIError(f"Malformed LangWatch search response for {self._error_path(response)}")
        skipped = (body.get("pagination") or {}).get("skipped")
        if skipped:
            self.logger.warning("LangWatch dropped %s trace(s) from a page that failed to serialize.", skipped)
        yield from traces

    def get_next_page_token(self, response: requests.Response, previous_token: Any) -> str | None:
        if previous_token is None:
            self._seen_scroll_ids = set()
        body = response.json()
        scroll_id = (body.get("pagination") or {}).get("scrollId")
        if not scroll_id:
            return None
        if not body["traces"]:
            # A page with no rows cannot advance the keyset; following its cursor would loop.
            return None
        if scroll_id in self._seen_scroll_ids:
            raise RuntimeError(f"LangWatch returned an already visited scrollId for stream '{self.name}'.")
        self._seen_scroll_ids.add(scroll_id)
        return scroll_id

    def get_records(self, context: dict | None) -> Iterable[tuple[dict, dict]]:
        for row in self.request_records(context):
            self.nested = {"spans": row.pop("spans", None) or [], "evaluations": row.pop("evaluations", None) or []}
            record = self.post_process(row, context)
            yield record, {"trace_id": record["trace_id"]}

    def post_process(self, row: dict, context: dict | None = None) -> dict:
        trace_id = row.get("trace_id")
        updated_at = (row.get("timestamps") or {}).get("updated_at")
        if not trace_id or not isinstance(updated_at, int):
            raise FatalAPIError(f"LangWatch trace {trace_id!r} has no trace_id or timestamps.updated_at.")
        row["updated_at"] = epoch_ms_to_iso(updated_at)
        return super().post_process(row, context)

    def get_child_context(self, record: dict, context: dict | None) -> dict:
        return context or {}


class _TraceChildStream(LangWatchStream):
    """Rows nested in each trace of the search response; no request of their own."""

    parent_stream_type = TracesStream
    path = ""
    context_key: str
    id_key: str

    def _get_state_partition_context(self, context: dict | None) -> None:
        # Full-table rows re-emitted whenever their trace changes: no per-trace state.
        return None

    def get_records(self, context: dict | None) -> Iterable[dict]:
        for row in self.parent_stream.nested[self.context_key]:
            if not row.get(self.id_key):
                raise FatalAPIError(
                    f"LangWatch trace {context['trace_id']!r} has a {self.name} row without {self.id_key}."
                )
            row["trace_id"] = context["trace_id"]
            yield self.post_process(row, context)

    @property
    def parent_stream(self) -> TracesStream:
        return self._tap.streams["traces"]


class SpansStream(_TraceChildStream):
    """Every span of a trace (LLM calls, tool calls, agents, RAG...), read with `includeSpans`,
    without its content payloads (see `SPAN_PARAMS_KEPT`)."""

    name = "spans"
    primary_keys = ["trace_id", "span_id"]
    context_key = "spans"
    id_key = "span_id"
    json_fields = ("error", "timestamps", "metrics", "params", "contexts")

    def post_process(self, row: dict, context: dict | None = None) -> dict:
        row.pop("input", None)
        row.pop("output", None)
        params = row.get("params")
        if isinstance(params, dict):
            row["params"] = {
                key: value if keep is None or not isinstance(value, dict) else {k: v for k, v in value.items() if k in keep}
                for key, value in params.items()
                if (keep := SPAN_PARAMS_KEPT.get(key, set())) is None or keep
            }
        return super().post_process(row, context)


class EvaluationsStream(_TraceChildStream):
    """Evaluations and guardrail results attached to a trace."""

    name = "evaluations"
    primary_keys = ["trace_id", "evaluation_id"]
    context_key = "evaluations"
    id_key = "evaluation_id"
    json_fields = ("inputs", "error", "timestamps")


class AnnotationsStream(LangWatchStream):
    """`GET /api/v1/annotations`: every human review in the project. The endpoint has no
    pagination and no date filter, so the stream is a full table each run."""

    name = "annotations"
    path = "/api/v1/annotations"
    primary_keys = ["id"]
    json_fields = ("scoreOptions",)

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        body = self.json_body(response)
        data = body.get("data")
        if not isinstance(data, list):
            raise FatalAPIError(f"Malformed LangWatch annotations response for {self._error_path(response)}")
        yield from data


STREAM_TYPES = [TracesStream, SpansStream, EvaluationsStream, AnnotationsStream]
