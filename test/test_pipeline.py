
from pymongo_aggregation.agg_operation import Agg_Operation, match, lookup, group, project, sort, range_match
from pymongo_aggregation.pipeline import Pipeline
from dateutil import parser
import pymongo
import unittest

class Test( unittest.TestCase):

    def setUp(self):
        self._client = pymongo.MongoClient()
        self._database = self._client[ "TEST_AGG"]
        self._collection = self._database[ "test_groups"]

    def test_pipeline(self):

        x = Pipeline()

        y = Pipeline( match())

        self.assertEqual( '[{\'$match\': {}}]', str(y))

        z = Pipeline( match = {})


    def test_append(self):

        m = match( {"a":"b"})
        l = lookup( {"c":"d"})
        g = group({ "one" : "two"})
        x = Pipeline( m, l )
        self.assertEqual( x, [m,l])
        x.append(g)
        self.assertEqual( x, [m,l,g])
        self.assertEqual( x, [ m, l, g])

    def test_op(self):
        m = match( {"a":"b"})
        l = lookup( {"c":"d"})
        g = group({ "one" : "two"})
        x = Pipeline( m, l, g )
        self.assertEqual( x, [ m, l, g])
        self.assertEqual( repr(x), "[" + repr(m) + ", " + repr(l) + ", " + repr(g)+ "]")

    def test_sort(self):
        p = Pipeline(match( {"group.status":"active"}),
                     project( { "_id" : 0, "group.name": 1}),
                     sort("group.name"))

        cursor = p.aggregate( self._collection)

        groups = list(cursor)
        self.assertEqual( groups[-1], {'group': {'name': 'mugh - MongoDB User Group in Hamburg'}})

        p = Pipeline(match( {"group.status":"active"}),
                     project( { "_id" : 0, "group.name": 1}),
                     sort(("group.name", pymongo.DESCENDING)))

        cursor = p.aggregate( self._collection)

        groups = list(cursor)
        self.assertEqual( groups[-1], {'group': {'name': 'Atlanta MongoDB User Group'}})

        cursor = p.aggregate( self._collection)
        self.assertEqual( 116, len(groups))

    def test_range_match(self):

        p = Pipeline( range_match( "group.created",
                                   start=parser.parse( "1-Jan-2015"),
                                   end=parser.parse( "31-Dec-2015")),
                      project( { "_id":0, "group.name": 1, "group.created" : 1 }))


        cursor = p.aggregate( self._collection)
        self.assertEqual( 4, len(list(cursor)))

if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()