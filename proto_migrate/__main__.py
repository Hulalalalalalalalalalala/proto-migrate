"""Command-line entry point: ``python3 -m proto_migrate --selftest``."""

from __future__ import annotations

import sys

from . import CURRENT_VERSION, VERSIONS, dumps, loads, migrate


def _selftest() -> None:
    message = {
        "order_id": "A-100",
        "amount": 12.5,
        "status": "new",
        "note": "first",
        "updated_at": 1700000000,
    }

    # Round-trip the current version.
    data = dumps(message)
    assert isinstance(data, bytes) and data.endswith(b"\n")
    assert b" " not in data.strip()
    decoded = loads(data)
    assert decoded == {"v": CURRENT_VERSION, **message}

    # Key order: v first, then fields in version order.
    assert data == (
        b'{"v":3,"order_id":"A-100","amount":12.5,'
        b'"status":"new","note":"first","updated_at":1700000000}\n'
    )

    # Decode older versions and migrate them forward.
    v1 = loads(b'{"v":1,"order_id":"A-1","amount":-0.0,"tags":["x","y"]}\n')
    assert v1 == {"v": 1, "order_id": "A-1", "amount": -0.0, "tags": ["x", "y"]}
    v2 = loads(b'{"v":2,"order_id":"A-2","amount":3,"tags":[],"status":"paid"}\n')
    assert v2["status"] == "paid"

    migrated = migrate(v1, 3)
    assert migrated == {
        "v": 3,
        "order_id": "A-1",
        "amount": -0.0,
        "status": "new",
        "note": "",
        "updated_at": 0,
    }
    back = migrate(migrated, 1)
    assert back == {"v": 1, "order_id": "A-1", "amount": -0.0, "tags": []}

    # -0.0 is preserved verbatim.
    assert b"-0.0" in dumps({**message, "amount": -0.0})

    # Error contract.
    for bad_call, exc in [
        (lambda: dumps({"v": 1, **message}), ValueError),
        (lambda: dumps("not a mapping"), ValueError),
        (lambda: dumps({**message, "amount": float("nan")}), ValueError),
        (lambda: dumps({**message, "amount": float("inf")}), ValueError),
        (lambda: loads("not bytes"), TypeError),
        (lambda: loads(b"not json"), ValueError),
        (lambda: loads(b'{"v":4,"order_id":"x","amount":1}'), ValueError),
        (lambda: loads(b'{"v":"3","order_id":"x","amount":1}'), ValueError),
        (lambda: loads(b'{"v":3,"order_id":"x"}'), ValueError),
        (lambda: migrate("not a mapping", 3), TypeError),
        (lambda: migrate({"order_id": "x"}, 3), TypeError),
        (lambda: migrate(decoded, 9), ValueError),
    ]:
        try:
            bad_call()
        except exc:
            pass
        else:  # pragma: no cover
            raise AssertionError(f"expected {exc.__name__}")

    assert VERSIONS == (1, 2, 3) and CURRENT_VERSION == 3


def main(argv: list[str] | None = None) -> int:
    args = sys.argv[1:] if argv is None else argv
    if args == ["--selftest"]:
        _selftest()
        print("ok")
        return 0
    print("usage: python3 -m proto_migrate --selftest", file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main())
