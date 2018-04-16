"""
Simple class to encapsulate MongoDB aggregation operations
Class name of sub-classes must match an Agg operation.

TO create new agg operations just create a sub-class that
matches the class name. If you want to use a different name for
the class name just make sure to override the name operator with
the correct name for the MongoDB Aggregation operation
"""

from collections import OrderedDict
import datetime

class Agg_Operation(object):

    subclasses = OrderedDict()

    def __init__(self, doc=None):
        if doc is None:
            self._doc = {}
        else:
            self._doc = doc

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        cls.subclasses[cls.__name__] = cls

    @staticmethod
    def ops():
        return Agg_Operation.subclasses

    def set_op(self, doc):
        self._doc = doc

    def get_op(self):
        return self._doc

    def __call__(self):
        return { self.op_name() : self._doc }

    def name(self):
        return self.__class__.__name__

    def op_name(self):
        return "$" + self.name()

    def __repr__(self):
        return "{" + '\'${}\': {}'.format( self.name(), self._doc) + "}"

class match(Agg_Operation):

    @staticmethod
    def date_range_query(date_field, start=None, end=None):

        if start is not None and not isinstance(start, datetime.datetime):
            raise ValueError( "{} is not and instance of datetime".format( start ))
        if end is not None and not isinstance(end, datetime.datetime):
            raise ValueError( "{} is not and instance of datetime".format( end ))

        range_query = None
        if start and end:
            range_query = {date_field: {"$gte": start, "$lte": end}}
        elif start:
            range_query = {date_field: {"$gte": start}}
        elif end:
            range_query = {date_field: {"$lte": end}}

        return range_query

class project(Agg_Operation):
    pass

class group(Agg_Operation):
    pass

class count(Agg_Operation):
    pass

class lookup(Agg_Operation):
    pass

class sort(Agg_Operation):
    pass

class sortByCount(Agg_Operation):
    pass

class unwind(Agg_Operation):
    pass

class redact(Agg_Operation):
    pass

class count(Agg_Operation):
    pass

class limit(Agg_Operation):
    pass

class skip(Agg_Operation):
    pass

class out(Agg_Operation):
    pass

class addFields(Agg_Operation):
    pass

class bucket(Agg_Operation):
    pass

class bucketAuto(Agg_Operation):
    pass

class collStats(Agg_Operation):
    pass

class currentOp(Agg_Operation):
    pass

class facet(Agg_Operation):
    pass

class geoNear(Agg_Operation):
    pass

class graphLookup(Agg_Operation):
    pass

class indexStats(Agg_Operation):
    pass

class listLocalSessions(Agg_Operation):
    pass

class listSessions(Agg_Operation):
    pass

class replaceRoot(Agg_Operation):
    pass

class sample(Agg_Operation):
    pass

class Example_for_Sample_Op_with_name(Agg_Operation):

    def name(self):
        return "sample"



