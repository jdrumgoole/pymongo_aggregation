import unittest

import pymag

"""
Default test cluster mongodb+srv://readonly:readonly@demodata.rgl39.mongodb.net/<dbname>?retryWrites=true&w=majority
use demo.zipcodes collection
"""

import pprint

import pymongo


def add_a_string():
    return "this is a string"


class TestOperators(unittest.TestCase):


    def setUp(self):
        cluster = "mongodb+srv://readonly:readonly@demodata.rgl39.mongodb.net/<dbname>?retryWrites=true&w=majority"
        client = pymongo.MongoClient(host=cluster)
        db = client["demo"]
        self.col = db["zipcodes"]

    def test_limit(self):
        p = pymag.Pipeline()
        limiter = pymag.limit(10)
        p.append(limiter)
        print(p)
        c = p.aggregate(self.col)
        for d in c:
            print(d)


    def test_function(self):

        p = pymag.Pipeline()
        func = pymag.function(pymag.JSCode(add_a_string, "bongo"))
        print("func")
        pprint.pprint(func())
        limiter = pymag.limit(10)
        adder = pymag.addFields({"new_field": func()})
        p.append(limiter)
        p.append(adder)
        pprint.pprint(p)
        c = p.aggregate(self.col)
        for d in c:
            print(d)

if __name__ == '__main__':
    unittest.main()
