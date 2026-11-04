"""LangWatch tap class."""

from __future__ import annotations

from hotglue_singer_sdk import Stream, Tap
from hotglue_singer_sdk.helpers.capabilities import AlertingLevel

from tap_langwatch.client import DEFAULT_API_URL
from tap_langwatch.streams import MAX_PAGE_SIZE, STREAM_TYPES


class TapLangWatch(Tap):
    """Singer tap for the LangWatch REST API."""

    name = "tap-langwatch"
    alerting_level = AlertingLevel.WARNING

    config_jsonschema = {
        "type": "object",
        "properties": {
            "api_key": {
                "type": "string",
                "minLength": 1,
                "secret": True,
                "description": "LangWatch API key (`sk-lw-...`) with Traces read permission, "
                "plus Annotations read for the annotations stream.",
            },
            "start_date": {
                "type": "string",
                "format": "date-time",
                "description": "Lower bound (last-modified time) for traces on the first run.",
            },
            "api_url": {
                "type": "string",
                "default": DEFAULT_API_URL,
                "description": "LangWatch base URL; change it for a self-hosted instance.",
            },
            "page_size": {
                "type": "integer",
                "minimum": 1,
                "maximum": MAX_PAGE_SIZE,
                "default": 200,
                "description": "Traces per search page (LangWatch caps a page at 1000 and may "
                "clamp it further by plan).",
            },
        },
        "required": ["api_key", "start_date"],
    }

    def discover_streams(self) -> list[Stream]:
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]


if __name__ == "__main__":
    TapLangWatch.cli()
