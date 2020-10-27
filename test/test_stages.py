"""
Basic unit tests for agg_operation
"""
from pymag import *
import unittest
import datetime


class Test(unittest.TestCase):

    def test_name(self):
        a = DocStage({})
        b = PositiveIntStage(1)
        c = StrStage("hello")
        d = match({})
        self.assertEqual(a.name, "DocStage")
        self.assertEqual(b.name, "PositiveIntStage")
        self.assertEqual(c.name, "StrStage")
        self.assertEqual(d.name, "match")

    def test_agg_op(self):

        op = AggStage()
        self.assertEqual(op.name, AggStage.__name__)

    def test_doc_op(self):

        op = DocStage()
        self.asserTrue(isinstance(op, DocStage))
        op = DocStage({"a": "b"})
        self.assertEqual(op.name, "DocStage")
        op = match({"a": "c"})
        self.assertEqual(op.name, "match")
        self.assertRaises(ValueError, match, 1) # not a dict

        op = match()
        self.assertEqual(op.op_name, "$match")
        self.assertEqual(str(op), "{'$match': {}}")
        self.assertEqual(repr(op), "match({})")

    def test_str_op(self):

        op = StrStage()
        self.assertTrue(isinstance(op, StrStage))
        op = StrStage("hello")
        self.assertEqual(op.arg, "hello")

    def test_positive_int_op(self):
        op = PositiveIntStage(123)
        self.assertEqual(op.arg, 123)

        self.assertRaises(ValueError, PositiveIntStage, "hello")
        self.assertRaises(ValueError, PositiveIntStage, -1)

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
        self.assertTrue(isinstance(op, match))
        self.assertEqual(op(), "{'$match': {}}")
        self.assertEqual(str(op), "{'$match': {}}")
        self.assertEqual(str(op), op())

    def test_name_override(self):

        op = Example_for_Sample_Op_with_name()
        self.assertEqual("{'$sample': {}}", op())

    def test_repr(self):
        op = lookup()
        self.assertEqual(repr(op), f"lookup({op.arg})")

    def test_ranged_match(self):
        now = datetime.datetime.utcnow()
        op = match.range_query(field="created",
                               start=datetime.datetime(2020, 10, 9, 16, 6, 55, 1))
        #print(op)
        result = "{'$match': {'created': {'$gte': datetime.datetime(2020, 10, 9, 16, 6, 55, 1)}}}"
        self.assertEqual(op(), result)

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
