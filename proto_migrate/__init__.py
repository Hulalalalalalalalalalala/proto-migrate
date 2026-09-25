"""Versioned message codec with forward migration.

Record fields: order_id (str), amount (number), tags (list of str),
status (str), note (str), updated_at (int).

Versions, ascending: 1, 2, 3. Version 3 is the current version written
by dumps; versions 1 and 2 are read-only.

  v1: order_id, amount, tags
  v2: order_id, amount, tags, status
  v3: order_id, amount, status, note, updated_at   (tags dropped)

Encoding is a compact JSON object plus a trailing newline: "v" first,
then the fields in that version's field order, with no whitespace
inside the object.
"""

from __future__ import annotations

import json
import math
from collections.abc import Mapping

__all__ = [
    "VERSIONS",
    "CURRENT_VERSION",
    "dumps",
    "loads",
    "migrate",
    "migrate_log_file",
    "read_log",
    "migrate_log_group",
    "read_log_group",
    "migrate_linked_logs",
    "read_linked_logs",
    "read_linked_logs_stream",
    "BadRecordError",
    "GroupBadRecordError",
    "LinkedBadReferenceError",
    "MigrationLockedError",
    "GroupMigrationResult",
    "LinkedMigrationResult",
    "MigrationResult",
    "run_cli",
    "run_group_cli",
    "run_linked_cli",
]

VERSIONS = (1, 2, 3)
CURRENT_VERSION = 3

_FIELDS = {
    1: ("order_id", "amount", "tags"),
    2: ("order_id", "amount", "tags", "status"),
    3: ("order_id", "amount", "status", "note", "updated_at"),
}


def _is_int(value):
    return isinstance(value, int) and not isinstance(value, bool)


def _is_amount(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _is_tags(value):
    return isinstance(value, list) and all(isinstance(item, str) for item in value)


_CHECKERS = {
    "order_id": lambda v: isinstance(v, str),
    "amount": _is_amount,
    "tags": _is_tags,
    "status": lambda v: isinstance(v, str),
    "note": lambda v: isinstance(v, str),
    "updated_at": _is_int,
}


def _default(name):
    if name == "status":
        return "new"
    if name == "note":
        return ""
    if name == "updated_at":
        return 0
    if name == "tags":
        return []
    raise ValueError(f"no default for field {name!r}")


def _validate_fields(message, version):
    for name in _FIELDS[version]:
        if name not in message:
            raise ValueError(f"missing field {name!r} for version {version}")
        if not _CHECKERS[name](message[name]):
            raise ValueError(
                f"field {name!r} has wrong type for version {version}"
            )
    amount = message["amount"]
    if isinstance(amount, float) and (math.isnan(amount) or math.isinf(amount)):
        raise ValueError("amount must be a finite number")


def _check_version(version):
    if not _is_int(version):
        raise ValueError("version key 'v' must be an integer")
    if version not in _FIELDS:
        raise ValueError(f"unsupported version: {version!r}")


def dumps(message):
    """Encode a field mapping (without "v") as the current version."""
    if not isinstance(message, Mapping):
        raise ValueError("message must be a mapping")
    if "v" in message:
        raise ValueError("message must not contain 'v'")
    _validate_fields(message, CURRENT_VERSION)
    payload = {"v": CURRENT_VERSION}
    for name in _FIELDS[CURRENT_VERSION]:
        payload[name] = message[name]
    text = json.dumps(
        payload,
        ensure_ascii=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    return (text + "\n").encode("utf-8")


def _reject_constant(value):
    raise ValueError(f"non-JSON constant: {value}")


def loads(data):
    """Decode bytes of any known version into a mapping including "v"."""
    if not isinstance(data, bytes):
        raise TypeError("loads expects bytes")
    try:
        obj = json.loads(data.decode("utf-8"), parse_constant=_reject_constant)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("invalid JSON payload") from exc
    if not isinstance(obj, dict):
        raise ValueError("payload must be a JSON object")
    if "v" not in obj:
        raise ValueError("payload is missing version key 'v'")
    _check_version(obj["v"])
    version = obj["v"]
    _validate_fields(obj, version)
    result = {"v": version}
    for name in _FIELDS[version]:
        result[name] = obj[name]
    return result


def migrate(message, target_version):
    """Convert a decoded message to the target version's shape."""
    if not isinstance(message, Mapping):
        raise TypeError("message must be a mapping")
    if "v" not in message:
        raise TypeError("message is missing version key 'v'")
    if not _is_int(target_version) or target_version not in _FIELDS:
        raise ValueError(f"unsupported target version: {target_version!r}")
    _check_version(message["v"])
    result = {"v": target_version}
    for name in _FIELDS[target_version]:
        if name in message:
            result[name] = message[name]
        else:
            result[name] = _default(name)
    return result


from .log_migration import (  # noqa: E402
    BadRecordError,
    MigrationResult,
    migrate_log_file,
    read_log,
    run_cli,
)
from .group_migration import (  # noqa: E402
    GroupBadRecordError,
    GroupMigrationResult,
    migrate_log_group,
    read_log_group,
    run_group_cli,
)
from .linked_migration import (  # noqa: E402
    LinkedBadReferenceError,
    LinkedMigrationResult,
    MigrationLockedError,
    migrate_linked_logs,
    read_linked_logs,
    read_linked_logs_stream,
    run_linked_cli,
)
