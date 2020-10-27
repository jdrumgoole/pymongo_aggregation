import unittest

import pymag


class MyTestCase(unittest.TestCase):
    """
    Default test cluster mongodb+srv://readonly:readonly@demodata.rgl39.mongodb.net/<dbname>?retryWrites=true&w=majority
    use demo.zipcodes collection
    """
    def test_pipeline_init(self):
        x = pymag.Pipeline()
        self.assertEqual(len(x), 0)

        x = pymag.Pipeline([pymag.match()])
        self.assertEqual(len(x), 1)

        self.assertRaises(ValueError, pymag.Pipeline, [1])
        self.assertRaises(ValueError, pymag.Pipeline, [pymag.out("outy"), pymag.match()])

    def test_match(self):
        x = pymag.Pipeline()
        self.assertEqual(len(x), 0)

        m = pymag.match()
        x = pymag.Pipeline([pymag.match()])
        self.assertEqual(len(x), 1)

    @staticmethod
    def random_field():
        return "Hello World"
    def test_function(self):
        p = pymag.Pipeline()
        p.append(pymag.match())
        p.append(pymag.addFields({"dummy" : pymag.func }))

if __name__ == '__main__':
    unittest.main()
