"""Versioned message codec with forward migration.

Record fields:
    order_id   str        order number
    amount     int/float  amount (NaN/Infinity rejected; -0.0 preserved)
    tags       list[str]  labels (versions 1 and 2 only)
    status     str        status (versions 2 and 3)
    note       str        note (version 3 only)
    updated_at int        update time (version 3 only)

Encoding: a compact JSON object (no whitespace) followed by a newline.
The "v" key is written first, then the fields in the version's order.
"""

from __future__ import annotations

import json
from collections.abc import Mapping

__all__ = ["VERSIONS", "CURRENT_VERSION", "dumps", "loads", "migrate"]

VERSIONS: tuple[int, ...] = (1, 2, 3)
CURRENT_VERSION: int = 3

# Fields written per version, in wire order.
_FIELDS: dict[int, tuple[str, ...]] = {
    1: ("order_id", "amount", "tags"),
    2: ("order_id", "amount", "tags", "status"),
    3: ("order_id", "amount", "status", "note", "updated_at"),
}

# Defaults used by migrate() when the target needs a field the source lacks.
# Values are zero-argument factories so mutable defaults stay fresh.
_DEFAULTS: dict[str, object] = {
    "tags": list,
    "status": lambda: "new",
    "note": lambda: "",
    "updated_at": lambda: 0,
}


def _is_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _is_number(value: object) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _validate_field(name: str, value: object) -> None:
    if name == "order_id" or name == "status" or name == "note":
        ok = isinstance(value, str)
    elif name == "amount":
        ok = _is_number(value)
    elif name == "tags":
        ok = isinstance(value, list) and all(isinstance(item, str) for item in value)
    elif name == "updated_at":
        ok = _is_int(value)
    else:  # pragma: no cover - unknown field name
        ok = False
    if not ok:
        raise ValueError(f"invalid value for field {name!r}: {value!r}")


def _check_version(value: object) -> int:
    if not _is_int(value):
        raise ValueError(f"version must be an integer, got {value!r}")
    if value not in VERSIONS:
        raise ValueError(f"unsupported version: {value!r}")
    return value


def _reject_constant(value: str) -> None:
    raise ValueError(f"invalid JSON constant: {value}")


def dumps(message: Mapping[str, object]) -> bytes:
    """Encode a field mapping (without "v") as the current version."""
    if not isinstance(message, Mapping):
        raise ValueError("message must be a mapping")
    if "v" in message:
        raise ValueError("message must not contain a 'v' key")
    fields = _FIELDS[CURRENT_VERSION]
    for name in fields:
        if name not in message:
            raise ValueError(f"missing field: {name!r}")
        _validate_field(name, message[name])
    record: dict[str, object] = {"v": CURRENT_VERSION}
    for name in fields:
        record[name] = message[name]
    text = json.dumps(
        record,
        ensure_ascii=True,
        allow_nan=False,
        separators=(",", ":"),
    )
    return (text + "\n").encode("utf-8")


def loads(data: bytes) -> dict[str, object]:
    """Decode any known version, returning a mapping with "v" and its fields."""
    if not isinstance(data, bytes):
        raise TypeError(f"loads() requires bytes, got {type(data).__name__}")
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(f"invalid UTF-8: {exc}") from exc
    try:
        obj = json.loads(text, parse_constant=_reject_constant)
    except ValueError as exc:
        raise ValueError(f"invalid JSON: {exc}") from exc
    if not isinstance(obj, dict):
        raise ValueError("message must be a JSON object")
    if "v" not in obj:
        raise ValueError("missing 'v' key")
    version = _check_version(obj["v"])
    result: dict[str, object] = {"v": version}
    for name in _FIELDS[version]:
        if name not in obj:
            raise ValueError(f"missing field: {name!r}")
        _validate_field(name, obj[name])
        result[name] = obj[name]
    return result


def migrate(message: Mapping[str, object], target_version: int) -> dict[str, object]:
    """Convert a decoded message to ``target_version``.

    Fields the target does not need are dropped; fields the target needs
    but the source lacks are filled with defaults.
    """
    if not isinstance(message, Mapping):
        raise TypeError("message must be a mapping")
    if "v" not in message:
        raise TypeError("message must contain a 'v' key")
    if not _is_int(target_version) or target_version not in VERSIONS:
        raise ValueError(f"unsupported target version: {target_version!r}")
    _check_version(message["v"])
    result: dict[str, object] = {"v": target_version}
    for name in _FIELDS[target_version]:
        if name in message:
            result[name] = message[name]
        elif name in _DEFAULTS:
            result[name] = _DEFAULTS[name]()
        else:
            raise ValueError(f"missing field with no default: {name!r}")
    return result
