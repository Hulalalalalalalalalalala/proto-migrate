import math
import unittest

from proto_migrate import CURRENT_VERSION, VERSIONS, dumps, loads, migrate

V3_MESSAGE = {
    "order_id": "A-100",
    "amount": 12.5,
    "status": "new",
    "note": "hi",
    "updated_at": 1700000000,
}


class TestDumps(unittest.TestCase):
    def test_round_trip_current_version(self):
        data = dumps(V3_MESSAGE)
        self.assertIsInstance(data, bytes)
        self.assertTrue(data.endswith(b"\n"))
        self.assertEqual(loads(data), {"v": 3, **V3_MESSAGE})

    def test_compact_and_ordered(self):
        data = dumps(V3_MESSAGE)
        self.assertEqual(
            data,
            b'{"v":3,"order_id":"A-100","amount":12.5,'
            b'"status":"new","note":"hi","updated_at":1700000000}\n',
        )

    def test_negative_zero_preserved(self):
        self.assertIn(b"-0.0", dumps({**V3_MESSAGE, "amount": -0.0}))

    def test_string_escaping(self):
        data = dumps({**V3_MESSAGE, "note": 'a"b\\c\nd'})
        self.assertIn(b'"a\\"b\\\\c\\nd"', data)

    def test_nan_and_infinity_rejected(self):
        for bad in (math.nan, math.inf, -math.inf):
            with self.assertRaises(ValueError):
                dumps({**V3_MESSAGE, "amount": bad})

    def test_v_key_rejected(self):
        with self.assertRaises(ValueError):
            dumps({"v": 3, **V3_MESSAGE})

    def test_non_mapping_rejected(self):
        for bad in ("x", 1, [1, 2], None):
            with self.assertRaises(ValueError):
                dumps(bad)

    def test_missing_or_wrong_type_rejected(self):
        with self.assertRaises(ValueError):
            dumps({"order_id": "x"})
        with self.assertRaises(ValueError):
            dumps({**V3_MESSAGE, "updated_at": 1.5})
        with self.assertRaises(ValueError):
            dumps({**V3_MESSAGE, "updated_at": True})


class TestLoads(unittest.TestCase):
    def test_requires_bytes(self):
        for bad in ('{"v":3}', 1, None, bytearray(b"{}")):
            with self.assertRaises(TypeError):
                loads(bad)

    def test_version_one(self):
        msg = loads(b'{"v":1,"order_id":"A","amount":1,"tags":["t"]}\n')
        self.assertEqual(msg, {"v": 1, "order_id": "A", "amount": 1, "tags": ["t"]})

    def test_version_two(self):
        msg = loads(b'{"v":2,"order_id":"A","amount":1,"tags":[],"status":"paid"}')
        self.assertEqual(msg["status"], "paid")

    def test_newer_version_rejected(self):
        with self.assertRaises(ValueError):
            loads(b'{"v":4,"order_id":"A","amount":1}')

    def test_unknown_and_non_integer_version_rejected(self):
        for raw in (b'{"v":0}', b'{"v":"1"}', b'{"v":1.5}', b'{"v":true}', b"{}"):
            with self.assertRaises(ValueError):
                loads(raw)

    def test_bad_json_rejected(self):
        for raw in (b"not json", b'{"v":3,}', b"[1,2]", b'"s"', b"\xff\xfe"):
            with self.assertRaises(ValueError):
                loads(raw)

    def test_missing_field_and_bad_types_rejected(self):
        with self.assertRaises(ValueError):
            loads(b'{"v":3,"order_id":"A","amount":1,"status":"new","note":""}')
        with self.assertRaises(ValueError):
            loads(b'{"v":1,"order_id":"A","amount":1,"tags":["a",2]}')
        with self.assertRaises(ValueError):
            loads(b'{"v":1,"order_id":"A","amount":"1","tags":[]}')
        with self.assertRaises(ValueError):
            loads(b'{"v":1,"order_id":"A","amount":NaN,"tags":[]}')
        with self.assertRaises(ValueError):
            loads(b'{"v":1,"order_id":"A","amount":Infinity,"tags":[]}')


class TestMigrate(unittest.TestCase):
    def test_forward_migration_defaults(self):
        msg = loads(b'{"v":1,"order_id":"A","amount":2,"tags":["x"]}')
        self.assertEqual(
            migrate(msg, 3),
            {"v": 3, "order_id": "A", "amount": 2,
             "status": "new", "note": "", "updated_at": 0},
        )

    def test_backward_migration_drops_and_defaults(self):
        msg = loads(dumps(V3_MESSAGE))
        self.assertEqual(
            migrate(msg, 1),
            {"v": 1, "order_id": "A-100", "amount": 12.5, "tags": []},
        )
        self.assertEqual(migrate(msg, 2)["status"], "new")

    def test_same_version(self):
        msg = loads(dumps(V3_MESSAGE))
        self.assertEqual(migrate(msg, 3), msg)

    def test_default_tags_list_is_fresh(self):
        msg = loads(dumps(V3_MESSAGE))
        a = migrate(msg, 1)
        a["tags"].append("x")
        self.assertEqual(migrate(msg, 1)["tags"], [])

    def test_bad_target_rejected(self):
        msg = loads(dumps(V3_MESSAGE))
        for bad in (0, 4, "3", None):
            with self.assertRaises(ValueError):
                migrate(msg, bad)

    def test_bad_message_rejected(self):
        with self.assertRaises(TypeError):
            migrate("nope", 3)
        with self.assertRaises(TypeError):
            migrate({"order_id": "A"}, 3)
        with self.assertRaises(ValueError):
            migrate({"v": 9, "order_id": "A"}, 3)


class TestConstants(unittest.TestCase):
    def test_versions(self):
        self.assertEqual(VERSIONS, (1, 2, 3))
        self.assertEqual(CURRENT_VERSION, 3)


if __name__ == "__main__":
    unittest.main()
