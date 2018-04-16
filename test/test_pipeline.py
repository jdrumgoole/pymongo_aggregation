
from pymongo_aggregation.agg_operation import Agg_Operation, match, lookup, group
from pymongo_aggregation.pipeline import Pipeline

import unittest

class Test( unittest.TestCase):

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

        print( x.pp())

if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()