"""Hex and bytes conversion utilities."""


def normalize_hex(value: str) -> str:
    stripped = value.strip().lower()
    return stripped[2:] if stripped.startswith("0x") else stripped


def is_hex(value: str) -> bool:
    try:
        int(value, 16)
        return True
    except ValueError:
        return False


def strip_0x(value: str) -> str:
    return value[2:] if value.startswith("0x") else value


def to_bytes(value: str) -> bytes:
    return bytes.fromhex(normalize_hex(value))


def to_hex(value: bytes) -> str:
    return "0x" + value.hex()
