"""Cursor pagination helpers."""


def encode_cursor(value: int | str) -> str:
    return str(value)


def decode_cursor(cursor: str) -> str:
    return cursor
