import unittest
from pymongo_aggregation.pipeline import Pipeline
from pymongo_aggregation.agg_operation import Agg_Operation, match, count_x
import pymongo


class Test(unittest.TestCase):

    def setUp(self):
        self._client = pymongo.MongoClient()
        self._db     = self._client[ "TEST_AGG"]
        self._col    = self._db["test_groups"]

    def test_aggregate(self):

        p = Pipeline( match(),count_x( "counter")).aggregate( self._col)

        for i in p:
            self.assertEqual( i["counter"], 116)

        p = Pipeline( match(),count_x="counter").aggregate( self._col)

        for i in p:
            self.assertEqual( i["counter"], 116)

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()