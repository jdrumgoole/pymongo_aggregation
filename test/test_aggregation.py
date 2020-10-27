
from pymag.stages import match, lookup, group, project, sort
from pymag.aggregation import Aggregation
from dateutil import parser
import pymongo
import unittest


class Test(unittest.TestCase):

    def setUp(self):
        self._client = pymongo.MongoClient()
        self._database = self._client["TEST_AGG"]
        self._collection = self._database["test_groups"]
        self.assertEqual( self._collection.count_documents({}), 116)

    def test_aggregation(self):

        y = Aggregation(match())
        self.assertEqual(str([match()]), str(y))

    def test_append(self):

        m = match({"a": "b"})
        l = lookup({"c": "d"})
        g = group({"one": "two"})
        x = Aggregation(m, l)
        self.assertEqual(list(x), [m, l])
        x.append(g)
        self.assertEqual(list(x), [m, l, g])
        self.assertEqual(list(x), [m, l, g])

    def test_op(self):
        m = match({"a": "b"})
        l = lookup({"c": "d"})
        g = group({"one": "two"})
        x = Aggregation(m, l, g )
        self.assertEqual(list(x), [m, l, g])
        self.assertEqual(str(x), str([m, l, g]))

    def test_sort(self):
        p = Aggregation(match({"group.status":"active"}),
                        project({"_id": 0, "group.name": 1}),
                        sort("group.name"))

        #print(p.aggregation_string)
        cursor = self._collection.aggregate(p)

        groups = list(cursor)
        self.assertEqual(groups[-1], {'group': {'name': 'mugh - MongoDB User Group in Hamburg'}})

        sorted = Aggregation(match({"group.status":"active"}),
                     project({"_id": 0, "group.name": 1}),
                     sort(("group.name", pymongo.DESCENDING)))

        unsorted = Aggregation(match({"group.status":"active"}),
                     project({"_id": 0, "group.name": 1}))

        unsorted_list = list(self._collection.aggregate(unsorted))
        sorted_list = list(self._collection.aggregate(sorted))

        self.assertNotEqual(unsorted_list, sorted_list)
        unsorted_list.sort(reverse=True, key=lambda x: x["group"]["name"])
        self.assertEqual(unsorted_list, sorted_list)
        self.assertEqual(sorted_list[-1], {'group': {'name': 'Atlanta MongoDB User Group'}})
        self.assertEqual(116, len(groups))

    def test_range_match(self):

        p = Aggregation(match.range_query("group.created",
                                          start=parser.parse("1-Jan-2015"),
                                          end=parser.parse("31-Dec-2015")),
                        project({"_id": 0, "group.name": 1, "group.created": 1 }))
        #print(f"Aggregation : {repr(p)}")
        cursor = self._collection.aggregate(p)
        #print(list(cursor))
        self.assertEqual(4, len(list(cursor)))


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
