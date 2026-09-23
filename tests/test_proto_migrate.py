import math
import unittest

import proto_migrate
from proto_migrate import CURRENT_VERSION, VERSIONS, dumps, loads, migrate


V3_RECORD = {
    "order_id": "ORD-42",
    "amount": 19.99,
    "status": "paid",
    "note": "gift",
    "updated_at": 1700000000,
}


class TestConstants(unittest.TestCase):
    def test_versions_ascending(self):
        self.assertEqual(VERSIONS, (1, 2, 3))
        self.assertEqual(tuple(sorted(VERSIONS)), VERSIONS)

    def test_current_version(self):
        self.assertEqual(CURRENT_VERSION, 3)
        self.assertIn(CURRENT_VERSION, VERSIONS)


class TestDumps(unittest.TestCase):
    def test_roundtrip(self):
        decoded = loads(dumps(V3_RECORD))
        self.assertEqual(decoded, {"v": 3, **V3_RECORD})

    def test_compact_with_newline_and_field_order(self):
        blob = dumps(V3_RECORD)
        self.assertTrue(blob.endswith(b"\n"))
        self.assertEqual(blob.count(b"\n"), 1)
        body = blob[:-1].decode("utf-8")
        self.assertNotIn(" ", body)
        self.assertEqual(
            body,
            '{"v":3,"order_id":"ORD-42","amount":19.99,'
            '"status":"paid","note":"gift","updated_at":1700000000}',
        )

    def test_negative_zero_preserved(self):
        blob = dumps({**V3_RECORD, "amount": -0.0})
        self.assertIn(b"-0.0", blob)
        decoded = loads(blob)
        self.assertEqual(decoded["amount"], 0.0)
        self.assertTrue(math.copysign(1.0, decoded["amount"]) < 0)

    def test_string_escaping(self):
        blob = dumps({**V3_RECORD, "note": 'a"b\\c\n'})
        self.assertIn(b'"a\\"b\\\\c\\n"', blob)

    def test_nan_and_infinity_rejected(self):
        for bad in (float("nan"), float("inf"), float("-inf")):
            with self.assertRaises(ValueError):
                dumps({**V3_RECORD, "amount": bad})

    def test_v_key_rejected(self):
        with self.assertRaises(ValueError):
            dumps({"v": 3, **V3_RECORD})

    def test_non_mapping_rejected(self):
        for bad in (None, 42, "x", [("order_id", "a")], b"bytes"):
            with self.assertRaises(ValueError):
                dumps(bad)

    def test_missing_or_mistyped_field_rejected(self):
        for key in V3_RECORD:
            with self.assertRaises(ValueError):
                dumps({k: v for k, v in V3_RECORD.items() if k != key})
        with self.assertRaises(ValueError):
            dumps({**V3_RECORD, "amount": "19.99"})
        with self.assertRaises(ValueError):
            dumps({**V3_RECORD, "updated_at": True})


class TestLoads(unittest.TestCase):
    def test_v1(self):
        msg = loads(b'{"v":1,"order_id":"A","amount":3,"tags":["x","y"]}\n')
        self.assertEqual(
            msg, {"v": 1, "order_id": "A", "amount": 3, "tags": ["x", "y"]}
        )

    def test_v2(self):
        msg = loads(b'{"v":2,"order_id":"A","amount":3.5,"tags":[],"status":"new"}')
        self.assertEqual(
            msg,
            {"v": 2, "order_id": "A", "amount": 3.5, "tags": [], "status": "new"},
        )

    def test_v3(self):
        msg = loads(dumps(V3_RECORD))
        self.assertEqual(msg["v"], 3)
        self.assertNotIn("tags", msg)

    def test_non_bytes_rejected(self):
        for bad in ("str", bytearray(b"x"), None, 1, {"v": 1}):
            with self.assertRaises(TypeError):
                loads(bad)

    def test_newer_version_rejected(self):
        with self.assertRaises(ValueError):
            loads(b'{"v":4,"order_id":"A","amount":1}')
        with self.assertRaises(ValueError):
            loads(b'{"v":99}')

    def test_unknown_older_version_rejected(self):
        with self.assertRaises(ValueError):
            loads(b'{"v":0,"order_id":"A","amount":1}')

    def test_non_integer_version_rejected(self):
        for blob in (b'{"v":"3"}', b'{"v":3.0}', b'{"v":true}', b'{"v":null}'):
            with self.assertRaises(ValueError):
                loads(blob)

    def test_missing_v_rejected(self):
        with self.assertRaises(ValueError):
            loads(b'{"order_id":"A","amount":1}')

    def test_invalid_json_rejected(self):
        for blob in (b"", b"not json", b'{"v":3,', b"[1,2]", b'"str"', b"3"):
            with self.assertRaises(ValueError):
                loads(blob)

    def test_nan_infinity_payload_rejected(self):
        with self.assertRaises(ValueError):
            loads(b'{"v":1,"order_id":"A","amount":NaN,"tags":[]}')
        with self.assertRaises(ValueError):
            loads(b'{"v":1,"order_id":"A","amount":Infinity,"tags":[]}')

    def test_missing_field_rejected(self):
        with self.assertRaises(ValueError):
            loads(b'{"v":1,"order_id":"A","amount":1}')
        with self.assertRaises(ValueError):
            loads(b'{"v":3,"order_id":"A","amount":1,"status":"new","note":""}')

    def test_wrong_type_rejected(self):
        with self.assertRaises(ValueError):
            loads(b'{"v":1,"order_id":5,"amount":1,"tags":[]}')
        with self.assertRaises(ValueError):
            loads(b'{"v":1,"order_id":"A","amount":"1","tags":[]}')
        with self.assertRaises(ValueError):
            loads(b'{"v":1,"order_id":"A","amount":1,"tags":["a",2]}')
        with self.assertRaises(ValueError):
            loads(
                b'{"v":3,"order_id":"A","amount":1,"status":"new",'
                b'"note":"","updated_at":1.5}'
            )


class TestMigrate(unittest.TestCase):
    V1 = {"v": 1, "order_id": "A", "amount": 2.5, "tags": ["t"]}
    V2 = {"v": 2, "order_id": "A", "amount": 2.5, "tags": ["t"], "status": "paid"}
    V3 = {
        "v": 3,
        "order_id": "A",
        "amount": 2.5,
        "status": "paid",
        "note": "n",
        "updated_at": 9,
    }

    def test_forward_defaults(self):
        self.assertEqual(
            migrate(self.V1, 3),
            {
                "v": 3,
                "order_id": "A",
                "amount": 2.5,
                "status": "new",
                "note": "",
                "updated_at": 0,
            },
        )

    def test_v2_to_v3_drops_tags(self):
        result = migrate(self.V2, 3)
        self.assertNotIn("tags", result)
        self.assertEqual(result["status"], "paid")
        self.assertEqual(result["note"], "")
        self.assertEqual(result["updated_at"], 0)

    def test_backward_defaults_and_drops(self):
        self.assertEqual(
            migrate(self.V3, 1),
            {"v": 1, "order_id": "A", "amount": 2.5, "tags": []},
        )
        self.assertEqual(
            migrate(self.V3, 2),
            {
                "v": 2,
                "order_id": "A",
                "amount": 2.5,
                "tags": [],
                "status": "paid",
            },
        )

    def test_same_version(self):
        self.assertEqual(migrate(self.V3, 3), self.V3)

    def test_bad_target(self):
        for target in (0, 4, -1, "3", None):
            with self.assertRaises(ValueError):
                migrate(self.V3, target)

    def test_non_mapping_or_missing_v(self):
        with self.assertRaises(TypeError):
            migrate([("v", 1)], 3)
        with self.assertRaises(TypeError):
            migrate({"order_id": "A"}, 3)

    def test_bad_source_version(self):
        with self.assertRaises(ValueError):
            migrate({"v": 7, "order_id": "A", "amount": 1}, 3)


class TestSelftestEntry(unittest.TestCase):
    def test_selftest_ok(self):
        import subprocess
        import sys

        proc = subprocess.run(
            [sys.executable, "-m", "proto_migrate", "--selftest"],
            capture_output=True,
            text=True,
        )
        self.assertEqual(proc.returncode, 0)
        self.assertEqual(proc.stdout, "ok\n")


if __name__ == "__main__":
    unittest.main()
