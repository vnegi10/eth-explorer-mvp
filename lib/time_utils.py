from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo


_CET = ZoneInfo("CET")


def format_unix_cet(value: object) -> object:
    if isinstance(value, int):
        return datetime.fromtimestamp(value, tz=_CET).strftime("%Y-%m-%d %H:%M:%S %Z")
    return value


def format_row_timestamps(row: dict, keys: tuple[str, ...] = ("timestamp", "block_timestamp")) -> dict:
    formatted = dict(row)
    for key in keys:
        if key in formatted:
            formatted[key] = format_unix_cet(formatted[key])
    return formatted
