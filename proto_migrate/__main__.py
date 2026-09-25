"""Command-line entry point: python3 -m proto_migrate --selftest."""

from __future__ import annotations

import sys

from . import CURRENT_VERSION, VERSIONS, dumps, loads, migrate, run_cli
from .compaction import run_compact_cli
from .group_migration import run_group_cli
from .linked_migration import run_linked_cli
from .rehearsal import run_rehearsal_cli


def _selftest():
    assert VERSIONS == (1, 2, 3)
    assert CURRENT_VERSION == 3

    record = {
        "order_id": "A-1",
        "amount": 9.5,
        "status": "new",
        "note": "hi",
        "updated_at": 7,
    }
    blob = dumps(record)
    assert blob.endswith(b"\n") and b" " not in blob
    decoded = loads(blob)
    assert decoded == {"v": 3, **record}

    v1 = loads(b'{"v":1,"order_id":"A-1","amount":9.5,"tags":["x"]}\n')
    assert v1 == {"v": 1, "order_id": "A-1", "amount": 9.5, "tags": ["x"]}
    v2 = migrate(v1, 2)
    assert v2 == {
        "v": 2,
        "order_id": "A-1",
        "amount": 9.5,
        "tags": ["x"],
        "status": "new",
    }
    v3 = migrate(v2, 3)
    assert v3 == {
        "v": 3,
        "order_id": "A-1",
        "amount": 9.5,
        "status": "new",
        "note": "",
        "updated_at": 0,
    }
    assert migrate(v3, 1) == {
        "v": 1,
        "order_id": "A-1",
        "amount": 9.5,
        "tags": [],
    }

    assert loads(dumps({**record, "amount": -0.0}))["amount"] == -0.0
    assert b"-0.0" in dumps({**record, "amount": -0.0})

    for bad_call in (
        lambda: dumps({"v": 3, **record}),
        lambda: dumps(["not", "a", "mapping"]),
        lambda: dumps({**record, "amount": float("nan")}),
        lambda: dumps({**record, "amount": float("inf")}),
        lambda: loads(b'{"v":4,"order_id":"x","amount":1}'),
        lambda: loads(b'{"v":"3"}'),
        lambda: loads(b"not json"),
        lambda: loads(b'{"v":3,"order_id":"x"}'),
        lambda: migrate({"v": 3, **record}, 9),
    ):
        try:
            bad_call()
        except ValueError:
            pass
        else:
            raise AssertionError("expected ValueError")

    for bad_type_call in (
        lambda: loads("not bytes"),
        lambda: migrate(["not", "a", "mapping"], 3),
        lambda: migrate(record, 3),
    ):
        try:
            bad_type_call()
        except TypeError:
            pass
        else:
            raise AssertionError("expected TypeError")


def main(argv=None):
    args = list(sys.argv[1:] if argv is None else argv)
    if args == ["--selftest"]:
        _selftest()
        print("ok")
        return 0
    if args and args[0] == "migrate-log":
        return run_cli(args[1:])
    if args and args[0] == "migrate-logs":
        return run_group_cli(args[1:])
    if args and args[0] == "migrate-linked-logs":
        return run_linked_cli(args[1:])
    if args and args[0] == "rehearse-linked-logs":
        return run_rehearsal_cli(args[1:])
    if args and args[0] == "compact-linked-logs":
        return run_compact_cli(args[1:])
    print(
        "usage: python3 -m proto_migrate --selftest\n"
        "       python3 -m proto_migrate migrate-log FILE [--strict|--skip]\n"
        "       python3 -m proto_migrate migrate-logs FILE [FILE ...] "
        "[--strict|--skip]\n"
        "       python3 -m proto_migrate migrate-linked-logs "
        "--group FILE [FILE ...] [--group ...] "
        "[--link SRC:DST:SRC_FIELD:DST_FIELD] [--strict|--skip]\n"
        "       python3 -m proto_migrate rehearse-linked-logs "
        "--group FILE [FILE ...] [--group ...] "
        "[--link SRC:DST:SRC_FIELD:DST_FIELD] [--strict|--skip]\n"
        "       python3 -m proto_migrate compact-linked-logs "
        "--group FILE [FILE ...] [--group ...] "
        "[--link SRC:DST:SRC_FIELD:DST_FIELD] [--strict|--skip]",
        file=sys.stderr,
    )
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
