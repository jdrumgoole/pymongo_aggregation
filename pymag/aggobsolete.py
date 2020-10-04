"""

AggObsolete
+++

``AggObsolete`` is a convenience class for constructing MongoDB Aggregation pipelines

@author: jdrumgoole

Helper class to make construction of aggregation pipelines in MongoDB
easier.

"""
import pprint
from datetime import datetime
from collections import OrderedDict

import pymongo
import json


class AggObsolete(object):
    '''
    A wrapper class for the MongoDB Aggregation framework (MongoDB 3.2)
    '''

    def __init__(self, collection, formatter="json"):
        '''
        Constructor json or python for format.
        '''
        self._collection = collection
        self._hasDollarOut = False
        self._cursor = None
        self._elapsed = None
        self._formatter = formatter
        self.clear()
        self._agg = []

    def __getattr__(self, op_name, op):
        return {op_name: op}

    @staticmethod
    def __limit(size):
        return {"$limit": size}

    @staticmethod
    def __sample(sampleSize):
        return {"$sample": {"$size": sampleSize}}

    @staticmethod
    def __match(matcher):
        AggObsolete.__typeCheckDict(matcher)
        return {"$match": matcher}

    @staticmethod
    def __project(projector):
        AggObsolete.__typeCheckDict(projector)
        return {"$project": projector}

    @staticmethod
    def __group(grouper):
        AggObsolete.__typeCheckDict(grouper)
        return {"$group": grouper}

    @staticmethod
    def __unwind(unwinder):
        # AggObsolete.__typeCheckDict( unwinder )
        return {"$unwind": unwinder}

    @staticmethod
    def __sort(sorter):
        # we typecheck higher up the stack
        return {"$sort": sorter}

    @staticmethod
    def __out(output):
        return {"$out": output}

    @staticmethod
    def __typeCheckDict(val):
        if not isinstance(val, dict):
            t = type(val)
            raise ValueError("Parameters must be dict objects: %s is a %s object " % (val, t))

    def __hasDollarOutCheck(self, op):
        if self._hasDollarOut:
            raise ValueError("Cannot have more aggregation pipeline operations after $out: operation '%s'" % op)

    def addLimit(self, size=None):

        if size is None:
            return self

        self.__hasDollarOutCheck("limit: %i" % size)
        self._agg.append(AggObsolete.__limit(size))

        return self

    def addSample(self, size=100):

        self.__hasDollarOutCheck("sample: %i" % size)
        self._agg.append(AggObsolete.__sample(size))

        return self

    def addMatch(self, matcher):

        self.__hasDollarOutCheck("match: %s" % matcher)
        self._agg.append(AggObsolete.__match(matcher))

        return self

    def addProject(self, projector):

        self.__hasDollarOutCheck("project: %s" % projector)
        self._agg.append(AggObsolete.__project(projector))

        return self

    def addGroup(self, grouper):

        self.__hasDollarOutCheck("group: %s" % grouper)
        self._agg.append(AggObsolete.__group(grouper))

        return self

    def addSort(self, sorter):
        '''
        Sorter can be a single dict or a list of dicts.
        '''

        self.__hasDollarOutCheck("$sort: %s" % sorter)

        if isinstance(sorter, Sorter):
            self._agg.append(sorter())
        else:
            raise ValueError("Parameter to addSort must of of class Sorter (type is '%s'" % type(sorter))
        return self

    def addUnwind(self, unwinder):

        self.__hasDollarOutCheck("$unwind: %s" % unwinder)
        self._agg.append(AggObsolete.__unwind(unwinder))

        return self

    def addOut(self, output=None):

        if output is None:
            return self

        if self._hasDollarOut:
            raise ValueError("Aggregation already has $out defined: %s" % self._agg)
        else:
            self._agg.append(AggObsolete.__out(output))
            self._hasDollarOut = True

        return self

    def clear(self):
        self._agg = []
        self._hasDollarOut = False
        self._elapsed = 0
        self._cursor = None

        return self

    def echo(self):
        print(self._agg)
        return self

    def formatter(self, output="json"):

        if output == "json":
            return self.json_format()
        elif output == "python":
            return self.python_format()
        else:
            raise ValueError("bad parmeter : output : %s" % output)

    @staticmethod
    def json_serial(obj):
        """JSON serializer for objects not serializable by default json code"""

        if isinstance(obj, datetime):
            serial = "ISODate( " + obj.isoformat() + " )"
            return serial
        raise TypeError("Type not serializable")

    def json_format(self):
        agg = "db." + self._collection.class_name + ".aggregate([\n"
        for i in self._agg:
            #            agg = agg + pprint.pformat( i ) + ",\n" 
            agg = agg + json.dumps(i, default=AggObsolete.json_serial) + ",\n"

        if agg.endswith(",\n"):
            agg = agg[:-2]

        return agg + '])\n'

    def python_format(self):
        agg = "db." + self._collection.class_name + ".aggregate( [\n"
        for i in self._agg:
            agg = agg + pprint.pformat(i) + ",\n"

        if agg.endswith(",\n"):
            agg = agg[:-2]

        return agg + '])\n'

    def __repr__(self):

        return self.formatter(self._formatter)

    def __str__(self):
        return self.__repr__()

    def addRangeMatch(self, date_field, start=None, end=None):

        query = None
        if start and end:
            query = {date_field: {"$gte": start, "$lte": end}}
        elif start:
            query = {date_field: {"$gte": start}}
        elif end:
            query = {date_field: {"$lte": end}}

        if query:
            self.addMatch(query)

        return self

    @staticmethod
    def cond(boolean_expr, thenClause, elseClause):  # $cond: { if: { $gte: [ "$qty", 250 ] }, then: 30, else: 20 }
        return {"$cond": {"if": boolean_expr, "then": thenClause, "else": elseClause}}

    @staticmethod
    def ifNull(null_value, non_null_value):
        return {"$ifNull": [null_value, non_null_value]}

    def cursor(self):
        return self._cursor

    def elapsed(self):
        return self._elapsed

    def aggregate(self):

        start = datetime.utcnow()
        self._cursor = self._collection.aggregate(self._agg)
        finish = datetime.utcnow()

        self._elapsed = finish - start

        return self._cursor

    def __call__(self):

        return self.aggregate()

    def create_view(self, database, view_name, collation=None):
        '''
        Create a view using the existing pipeline constructed within the class
        '''

        if collation is None:
            return database.command({"view": view_name,
                                     "viewOn": self._collection.class_name,
                                     "pipeline": self._agg})
        else:
            return database.command({"view": view_name,
                                     "viewOn": self._collection.class_name,
                                     "pipeline": self._agg,
                                     "collation": collation})

    def tee(self, output):
        '''
        Iterator over the aggregator and produce a copy in output
        '''

        for i in self.aggregate():
            output.append(i)
            yield i

    def simple_print(self):
        cursor = self.aggregate()
        for i in cursor:
            pprint.pprint(i)
