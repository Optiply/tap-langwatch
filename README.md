# tap-langwatch

Singer tap for the [LangWatch REST API](https://langwatch.ai/docs/api-reference/traces/overview), built on `hotglue-singer-sdk`. LangWatch is LLM observability, so this tap feeds no Optiply inventory entity: it extracts the traces of the Optiply agents (with their spans and evaluations) and the human annotations on them, for internal analysis.

The inputs and outputs in `traces` and `spans` are customers' actual conversations with the agent. Load this tap into its own restricted dataset.

## Config

| Key | Required | Description |
| --- | --- | --- |
| `api_key` | yes (secret) | LangWatch API key (`sk-lw-...`), sent as `X-Auth-Token`. Needs Traces read; Annotations read for the `annotations` stream. A key scoped to one project resolves it on its own. |
| `start_date` | yes | Lower bound (last-modified time) for `traces` on the first run. |
| `api_url` | no (default `https://app.langwatch.ai`) | Base URL, for a self-hosted instance. |
| `page_size` | no (default 200, max 1000) | Traces per search page. LangWatch may clamp it by plan; the scroll continues regardless. When `spans` is selected the page is capped at 10 (see below). |

See `config.example.json`.

## Streams

| Stream | Endpoint | Primary key | Replication | Notes |
| --- | --- | --- | --- | --- |
| `traces` | `POST /api/v1/traces/search` | `trace_id` | incremental `updated_at` | `dateField: "updated"`, `format: "json"`. One row per trace: metadata (`user_id`, `customer_id`, `thread_id`, `labels`, custom keys), timestamps, `input`/`output`, `metrics` (cost, tokens), `error`, `events` (SDK feedback such as `thumbs_up_down`), `platformUrl`. `updated_at` is `timestamps.updated_at` as an ISO string (the bookmark). `spans` and `evaluations` are removed from the row and emitted as the streams below. |
| `spans` | nested in each trace (`includeSpans: true`) | `trace_id`, `span_id` | full table, child | Every span: `type` (`llm`, `tool`, `agent`, `rag`, ...), `name`, `parent_id`, `model`, `vendor`, `error`, `timestamps`, `metrics`, and a pruned `params` (`tool`, `metadata`, `event`, `llm.provider`/`finish_reason`, `gen_ai.operation`/`request`/`response`/`usage`). Span `input`/`output` and the content keys of `params` (`langwatch.*`, `llm.input_messages`/`system`/`tools`, `gen_ai.conversation`/`input`/`output`/`system_instructions`) are dropped: they re-attach the whole conversation to every span (measured 2026-11-04: 15 MB per trace, 1% of it without them), and the conversation is already in `traces`. Re-emitted whenever the trace changes. `includeSpans` is only sent when this stream is selected. |
| `evaluations` | nested in each trace | `trace_id`, `evaluation_id` | full table, child | Evaluations and guardrail results (`name`, `status`, `passed`, `score`, `label`, `details`, `is_guardrail`). Re-emitted whenever the trace changes. |
| `annotations` | `GET /api/v1/annotations` | `id` | full table | Human reviews (`isThumbsUp`, `comment`, `scoreOptions`, `expectedOutput`, `traceId`). The endpoint has no pagination and no date filter, so every run reads the whole list. |

Not extracted:

- `GET /api/v1/thread/{id}`: every trace carries `metadata.thread_id`.
- `GET /api/v1/traces/{traceId}`: the search returns the same trace shape.
- Span-level inputs and outputs (see `spans` above). Raise `SPAN_PARAMS_KEPT` in `streams.py` if a field is needed.
- Write and share endpoints (metadata PATCH, share/unshare, annotation create/patch/delete, event tracking).
- Deletes. LangWatch exposes no deleted-trace feed; a span or evaluation removed from a trace is not emitted (the trace itself is re-emitted, and a target that replaces per trace can reconcile).

## Behavior

- **Window:** each run searches traces last modified between the bookmark (or `start_date`) and the sync start, inclusive on both sides. The bookmark becomes the largest `updated_at` delivered, never the requested end: LangWatch may cover less than `endDate` in one scroll (`pagination.updatedThrough`), and a trace modified while paging lands in the next window. A trace updated exactly at the bookmark is delivered again, so consumers deduplicate by `trace_id`. An empty window leaves the bookmark unchanged.
- **Pagination:** `pagination.scrollId` is sent back until it is absent. A repeated `scrollId` fails the stream; a page with no rows ends the listing. `pagination.skipped` (traces LangWatch failed to serialize) is logged as a warning.
- **Known LangWatch gap:** an in-place edit of an existing annotation does not advance the trace's `updated_at` ([langwatch#4736](https://github.com/langwatch/langwatch/issues/4736)); new and removed annotations do. The `annotations` stream is a full read, so it is not affected.
- **Child streams** keep no per-trace state entries.
- **HTTP errors:**
  - 401 raises `InvalidCredentialsError` and is not retried.
  - 400, 403, 404, 422 and other 4xx responses are fatal; 403 names the stream whose permission is missing.
  - 429 waits for `Retry-After` (capped at 120 s) and then backs off.
  - 429, 5xx (incl. LangWatch 503 `clickhouse_overloaded` and Cloudflare 524) and network errors are retried up to 8 attempts, waiting 5, 10, 20, 40, 80, 120, 120 s (no jitter; ~6.5 min in total) before giving up.
  - Requests time out after 10 s to connect and 300 s to read.
- **Span pages:** LangWatch answers 500 to `includeSpans` pages of 50 traces and ~150 MB for 10, so span-inclusive searches use `min(page_size, 10)` (`SPAN_PAGE_SIZE`). Expect roughly one 10 s request per 10 traces when `spans` is selected.
- **Rate limits:** LangWatch documents none for these endpoints; 429s are handled by the retry policy above.

## Development

```bash
uv sync
uv run pytest
uv run tap-langwatch --config config.json --discover > catalog.json
uv run tap-langwatch --config config.json --catalog catalog.json
```

The JSON schemas in `tap_langwatch/schemas/` follow the trace, span, evaluation and annotation shapes in LangWatch's open-source contract (`modules/trace/contract/src/trace-format.schemas.ts`, `GET /api/v1/annotations` OpenAPI). Every field is nullable. Nested objects and arrays (`metadata`, `timestamps`, `metrics`, `error`, `input`/`output`, `events`, `contexts`, span `params`, evaluation `inputs`, annotation `scoreOptions`) are emitted as **JSON text** (`json_fields` on each stream, schema type `string`): Hotglue's CSV stage writes Python reprs for real objects, which BigQuery cannot parse. Read them with `JSON_EXTRACT`/`PARSE_JSON`.

Upserts are the target's job: the tap declares the primary keys (`trace_id`; `trace_id, span_id`; `trace_id, evaluation_id`; `id`), and Hotglue's target-bigquery applies them only with `"replication_method": "incremental"` (default `append`); see https://docs.hotglue.com/connectors/bigquery.
