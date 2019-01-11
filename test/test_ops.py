"""
Basic unit tests for agg_operation
"""
from pymonager.ops import AggOperation, DocOperation, \
    match, Example_for_Sample_Op_with_name, lookup, count, sort, limit, out
import unittest
import datetime


class Test( unittest.TestCase):

    def test_agg_op(self):

        op=AggOperation()
        self.assertEqual(op.name, AggOperation.__name__)

    def test_doc_op(self):

        op = DocOperation({"a": "b"})
        self.assertEqual( op.name, "DocOperation")
        op = match({"a": "c"})
        self.assertEqual(op.name, "match")
        self.assertRaises(ValueError, match, 1) # not a dict

        op = match()
        self.assertEqual(op.op_name, "$match")
        self.assertEqual(str(op), "{'$match': {}}")
        self.assertEqual(repr(op), "match({})")

    def test_limit_op(self):
        self.assertRaises(ValueError, limit, -1)
        self.assertRaises(ValueError, limit, "hello")
        self.assertRaises(ValueError, limit, 1.1)

    def test_out_op(self):
        self.assertRaises(ValueError, out, "")
        self.assertRaises(ValueError, out, "$")
        self.assertRaises(ValueError, out, "system.")

    def test_sort(self):
        op=sort()
        self.assertEqual(op, {"$sort": {}})
        op=sort(("grouo", 1))
        self.assertEqual(op, {"$sort": {"group": 1}})
        op=sort(group=1)
        self.assertEqual(op, {"$sort": {"group": 1}})
        op.add(files=-1)
        self.assertEqual(op, {"$sort": {"group": 1, "files": -1}})

    def test_op(self):
        op = match()

        self.assertEqual(op, {"$match": {}})

    def test_name_override(self):

        op = Example_for_Sample_Op_with_name()
        self.assertEqual({"$sample": {}}, op)

    def test_repr(self):
        op = lookup()
        self.assertEqual(repr(op), f"lookup({op.arg})")

    def test_ranged_match(self):
        now = datetime.datetime.utcnow()
        op = match.time_range_query(date_field="created",
                                    start=now)

        self.assertEqual(op, {'$match': {'created': {'$gte': now}}})

    def test_count(self):

        op=count("counter")
        #print(op)
        self.assertRaises(ValueError, count, None)

    def test_sort(self):

        op = sort(name=1, date=-1)
        # print(op)


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
