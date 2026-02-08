from __future__ import annotations

from decimal import Decimal


_WEI_PER_ETH = Decimal(10**18)


def wei_string_to_eth_display(wei_value: object, decimals: int = 8) -> str | None:
    if wei_value is None:
        return None
    if isinstance(wei_value, str):
        text = wei_value.strip()
        if not text:
            return None
        wei_int = int(text)
    elif isinstance(wei_value, int):
        wei_int = wei_value
    else:
        return None
    eth = Decimal(wei_int) / _WEI_PER_ETH
    out = f"{eth:,.{decimals}f}".rstrip("0").rstrip(".")
    return f"{out} ETH" if out else "0 ETH"


def wei_hex_to_eth_display(hex_value: object, decimals: int = 8) -> str | None:
    if not isinstance(hex_value, str) or not hex_value:
        return None
    wei_int = int(hex_value, 16)
    eth = Decimal(wei_int) / _WEI_PER_ETH
    text = f"{eth:,.{decimals}f}".rstrip("0").rstrip(".")
    return text if text else "0"


def add_value_eth(row: dict, src_key: str = "value_binary_hex", dst_key: str = "value_eth") -> dict:
    out = dict(row)
    out[dst_key] = wei_hex_to_eth_display(out.get(src_key))
    return out


def format_value_string_as_eth(row: dict, key: str = "value_string") -> dict:
    out = dict(row)
    if key in out:
        out[key] = wei_string_to_eth_display(out.get(key))
    return out


def wei_int_to_eth_display(wei_value: object, decimals: int = 8) -> str | None:
    if not isinstance(wei_value, int):
        return None
    eth = Decimal(wei_value) / _WEI_PER_ETH
    out = f"{eth:,.{decimals}f}".rstrip("0").rstrip(".")
    return f"{out} ETH" if out else "0 ETH"
