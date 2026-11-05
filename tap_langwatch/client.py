"""Shared request, retry and error handling for LangWatch streams."""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import backoff
import requests
from hotglue_etl_exceptions import InvalidCredentialsError
from hotglue_singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from hotglue_singer_sdk.streams import RESTStream
from hotglue_singer_sdk.helpers._network import giveup_oserror_not_transient_network

SCHEMAS_DIR = Path(__file__).parent / "schemas"
DEFAULT_API_URL = "https://app.langwatch.ai"
REQUEST_TIMEOUT = (10, 300)
MAX_RETRY_AFTER_SECONDS = 120
ERROR_BODY_LIMIT = 500


def epoch_ms_to_iso(value: int) -> str:
    """Fixed-width UTC ISO timestamp (`2024-06-01T00:00:00.000Z`), so bookmarks compare as strings."""
    stamp = datetime.fromtimestamp(value / 1000, tz=timezone.utc)
    return stamp.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def iso_to_epoch_ms(value: str) -> int:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp() * 1000)


class LangWatchStream(RESTStream):
    """LangWatch REST endpoint authenticated with the project API key (`X-Auth-Token`)."""

    #: Nested fields emitted as JSON text (schema type string): Hotglue's CSV stage would
    #: otherwise write Python reprs, which BigQuery cannot parse.
    json_fields: tuple[str, ...] = ()

    def post_process(self, row: dict, context: dict | None = None) -> dict:
        for key in self.json_fields:
            if row.get(key) is not None:
                row[key] = json.dumps(row[key], ensure_ascii=False, separators=(",", ":"))
        return row

    @property
    def url_base(self) -> str:
        return self.config.get("api_url", DEFAULT_API_URL).rstrip("/")

    @property
    def schema_filepath(self) -> Path:
        return SCHEMAS_DIR / f"{self.name}.json"

    @property
    def http_headers(self) -> dict:
        return {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "X-Auth-Token": self.config["api_key"],
        }

    @property
    def timeout(self) -> tuple[int, int]:
        return REQUEST_TIMEOUT

    def validate_response(self, response: requests.Response) -> None:
        status = response.status_code
        if status == 401:
            raise InvalidCredentialsError(
                f"LangWatch rejected the API key for {self._error_path(response)} (401)."
            )
        if status == 429:
            self._wait_retry_after(response)
            raise RetriableAPIError(self._error_message(response), response)
        if status >= 500:
            raise RetriableAPIError(self._error_message(response), response)
        if status == 403:
            raise FatalAPIError(
                f"{self._error_message(response)} The API key lacks the permission for stream "
                f"'{self.name}'; grant it in LangWatch (Settings > API Keys) or deselect the stream."
            )
        if status >= 400:
            raise FatalAPIError(self._error_message(response))
        if status != 200:
            raise FatalAPIError(f"Unexpected {status} from LangWatch for {self._error_path(response)}")

    def _wait_retry_after(self, response: requests.Response) -> None:
        header = response.headers.get("Retry-After")
        if header is None:
            return
        try:
            seconds = float(header)
        except ValueError:
            return
        time.sleep(min(max(seconds, 0.0), MAX_RETRY_AFTER_SECONDS))

    @staticmethod
    def _error_path(response: requests.Response) -> str:
        return urlparse(response.url).path

    def _error_message(self, response: requests.Response) -> str:
        return (
            f"{response.status_code} {response.reason} from LangWatch for "
            f"{self._error_path(response)}: {response.text[:ERROR_BODY_LIMIT]}"
        )

    # LangWatch answers 503 `clickhouse_overloaded` ("retry in a few seconds") and Cloudflare 524
    # under load (job r4he1pua, 2026-11-05). The SDK's default full jitter turned the schedule
    # into 0.2-10 s waits, so the decorator is rebuilt without jitter: 5, 10, 20, 40, 80, 120, 120 s.
    def backoff_wait_generator(self):
        return backoff.expo(base=2, factor=5, max_value=120)

    def backoff_max_tries(self) -> int:
        return 8

    def request_decorator(self, func):
        return backoff.on_exception(
            self.backoff_wait_generator,
            self.backoff_exceptions(),
            max_tries=self.backoff_max_tries,
            on_backoff=self.backoff_handler,
            giveup=giveup_oserror_not_transient_network,
            jitter=None,
        )(func)

    def json_body(self, response: requests.Response) -> dict[str, Any]:
        try:
            body = response.json()
        except ValueError as ex:
            raise FatalAPIError(f"Non-JSON LangWatch response for {self._error_path(response)}") from ex
        if not isinstance(body, dict):
            raise FatalAPIError(f"Malformed LangWatch response for {self._error_path(response)}")
        return body
