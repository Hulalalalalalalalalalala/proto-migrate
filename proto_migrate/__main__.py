"""Command-line entry point.

    python3 -m proto_migrate --selftest
    python3 -m proto_migrate migrate-log FILE [--mode strict|skip]
"""

from __future__ import annotations

import sys

from . import CURRENT_VERSION, VERSIONS, dumps, loads, migrate
from .log_migrate import (
    EXIT_BAD_RECORD,
    LOG_SKIP,
    LOG_STRICT,
    BadRecordError,
    migrate_log,
)


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


def _migrate_log(argv):
    mode = LOG_STRICT
    positional = []
    i = 0
    while i < len(argv):
        arg = argv[i]
        if arg in ("--mode", "-m"):
            i += 1
            if i >= len(argv):
                print("migrate-log: --mode requires a value", file=sys.stderr)
                return 2
            mode = argv[i]
        elif arg.startswith("--mode="):
            mode = arg.split("=", 1)[1]
        elif arg in ("-h", "--help"):
            print(
                "usage: python3 -m proto_migrate migrate-log FILE "
                "[--mode strict|skip]",
                file=sys.stderr,
            )
            return 2
        else:
            positional.append(arg)
        i += 1

    if mode not in (LOG_STRICT, LOG_SKIP):
        print(
            f"migrate-log: invalid mode {mode!r} (expected 'strict' or 'skip')",
            file=sys.stderr,
        )
        return 2
    if len(positional) != 1:
        print(
            "usage: python3 -m proto_migrate migrate-log FILE "
            "[--mode strict|skip]",
            file=sys.stderr,
        )
        return 2

    path = positional[0]
    try:
        result = migrate_log(path, mode=mode, audit_stream=sys.stderr.buffer)
    except FileNotFoundError:
        print(f"migrate-log: file not found: {path}", file=sys.stderr)
        return 2
    except BadRecordError as exc:
        # Strict mode: first bad line, original file untouched.
        print(f"migrate-log: {exc}", file=sys.stderr)
        return EXIT_BAD_RECORD
    except OSError as exc:
        print(f"migrate-log: {exc}", file=sys.stderr)
        return 2

    what = "migrated" if result.changed else "already up to date"
    print(
        f"{what}: {result.total_lines} lines, "
        f"{result.migrated_lines} converted, {result.skipped} skipped"
    )
    return 0


def main(argv=None):
    args = list(sys.argv[1:] if argv is None else argv)
    if args == ["--selftest"]:
        _selftest()
        print("ok")
        return 0
    if args and args[0] == "migrate-log":
        return _migrate_log(args[1:])
    print("usage: python3 -m proto_migrate --selftest", file=sys.stderr)
    print(
        "       python3 -m proto_migrate migrate-log FILE "
        "[--mode strict|skip]",
        file=sys.stderr,
    )
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
