"""
Simple class to encapsulate MongoDB aggregation operations
Class name of sub-classes must match an Agg operation.

TO create new agg operations just create a sub-class that
matches the class name. If you want to use a different name for
the class name just make sure to override the name operator with
the correct name for the MongoDB Aggregation operation
"""

import datetime
from collections import OrderedDict

import pymongo


class Agg_Operation(object):

    subclasses = OrderedDict()

    @staticmethod
    def ops():
        return Agg_Operation.subclasses

    def name(self):
        return self.__class__.__name__

    def op_name(self):
        return "$" + self.name()

class Doc_Operation(Agg_Operation):
    """
    Superclass for all MongoDB Aggregation operations
    """

    subclasses = OrderedDict()

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        cls.subclasses[cls.__name__] = cls

    def __init__(self, doc=None):
        if doc is None:
            self._doc = {}
        else:
            self._doc = doc

    def set_op(self, doc):
        self._doc = doc

    def get_op(self):
        return self._doc

    def __call__(self):
        return { self.op_name() : self._doc }

    def __repr__(self):
        return "{" + '\'${}\': {}'.format( self.name(), self._doc) + "}"


class Value_Operation(Agg_Operation):

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        cls.subclasses[cls.__name__] = cls

    def __init__(self, value):
        self._value = value

    def set_value(self, value):
        self._value = value

    def get_value(self):
        return self._value

    def __call__(self):
        return { self.op_name() : self._value }

class match(Doc_Operation):
    pass

    # @staticmethod
    # def date_range_query(date_field, start=None, end=None):
    #
    #     if start is not None and not isinstance(start, datetime.datetime):
    #         raise ValueError( "{} is not and instance of datetime".format( start ))
    #     if end is not None and not isinstance(end, datetime.datetime):
    #         raise ValueError( "{} is not and instance of datetime".format( end ))
    #
    #     range_query = None
    #     if start and end:
    #         range_query = {date_field: {"$gte": start, "$lte": end}}
    #     elif start:
    #         range_query = {date_field: {"$gte": start}}
    #     elif end:
    #         range_query = {date_field: {"$lte": end}}
    #
    #     return range_query

class range_match(match):
    """
    Create a match operator specifying that a field fall between two date
    ranges *start* and *end*.
    """

    def __init__(self, field, start=None, end=None):
        if start is not None and not isinstance(start, datetime.datetime):
            raise ValueError( "{} is not and instance of datetime".format( start ))
        if end is not None and not isinstance(end, datetime.datetime):
            raise ValueError( "{} is not and instance of datetime".format( end ))

        self._doc = {field:{}}
        if start and end:
            self._doc = {field: {"$gte": start, "$lte": end}}
        elif start:
            self._doc = {field: {"$gte": start}}
        elif end:
            self._doc = {field: {"$lte": end}}

    def name(self):
        """
        Override the use of the class name as the name of the class so we parse the right
        operator
        :return: "match"
        """
        return "match"

class project(Doc_Operation):
    pass

class group(Doc_Operation):
    pass

class count_x(Doc_Operation):
    """
    Disambiguate from count function
    """

    def __init__(self, count_field):
        if isinstance( count_field, str):
            if count_field:
                self._doc = count_field
            else:
                raise ValueError( "count_field cannot be None")
        else:
            raise ValueError( "count_field must be a string (str type)")

    def op_name(self):
        return "$count"

class lookup(Doc_Operation):
    pass

class sort(Doc_Operation):
    '''
    Required for ordered sorting of fields as python dictionaries do not
    guarantee to maintain insertion order. Sorted fields are maintained
    in an ``OrderedDict`` class that ensures order is maintained.
    '''

    def __init__(self, *args):
        '''
        parameters are key="asending" or key="descending"
        Can use kwargs in the form field=pymongo.ASCENDING because field may
        be a dotted string e.g. "group.name".
        '''
        self._doc = OrderedDict()

        self.add(args)

    def add(self, sorts):
        for i in sorts:
            if isinstance( i, tuple):
                self.add_sort(i[0], i[1])
            else:
                self.add_sort( i, pymongo.ASCENDING)

    def sort_fields(self):
        return self._doc.keys()

    def sort_items(self):
        return self._doc.items()

    def sort_directions(self):
        return self._doc.values()

    def add_sort(self, field, sortOrder=pymongo.ASCENDING):
        if sortOrder in [pymongo.ASCENDING, pymongo.DESCENDING]:
            self._doc[field] = sortOrder
        else:
            raise ValueError("Invalid sort order must be pymongo.ASCENDING or pymongo.DESCENDING")

class sortByCount(Doc_Operation):
    pass

class unwind(Doc_Operation):
    pass

class redact(Doc_Operation):
    pass

class count(Value_Operation):
    pass

class limit(Value_Operation):
    pass

class skip(Value_Operation):
    pass

class out(Value_Operation):
    pass

class addFields(Doc_Operation):
    pass

class bucket(Doc_Operation):
    pass

class bucketAuto(Doc_Operation):
    pass

class collStats(Doc_Operation):
    pass

class currentOp(Doc_Operation):
    pass

class facet(Doc_Operation):
    pass

class geoNear(Doc_Operation):
    pass

class graphLookup(Doc_Operation):
    pass

class indexStats(Doc_Operation):
    pass

class listLocalSessions(Doc_Operation):
    pass

class listSessions(Doc_Operation):
    pass

class replaceRoot(Doc_Operation):
    pass

class sample(Doc_Operation):
    pass

class Example_for_Sample_Op_with_name(Doc_Operation):

    def name(self):
        return "sample"

# class Sorter(object):
#     '''
#     Required for ordered sorting of fields as python dictionaries do not
#     guarantee to maintain insertion order. Sorted fields are maintained
#     in an ``OrderedDict`` class that ensures order is maintained.
#     '''
#
#     def __init__(self, **kwargs):
#         '''
#         parameters are key="asending" or key="descending"
#         '''
#         self._sorted = {}
#         self._sorted["$sort"] = OrderedDict()
#
#         self.add(kwargs)
#
#     def add(self, sorts):
#         for k, v in sorts.items():
#             self.add_sort(k, v)
#
#     def sort_fields(self):
#         return self._sorted["$sort"].keys()
#
#     def sort_items(self):
#         return self._sorted["$sort"].items()
#
#     def sort_directions(self):
#         return self._sorted["$sort"].values()
#
#     def sorts(self):
#         return self._sorted
#
#     def add_sort(self, field, sortOrder=pymongo.ASCENDING):
#         if sortOrder in [pymongo.ASCENDING, pymongo.DESCENDING]:
#             self._sorted["$sort"][field] = sortOrder
#         else:
#             raise ValueError("Invalid sort order must be pymongo.ASCENDING or pymongo.DESCENDING")
#
#     def __call__(self):
#         return self._sorted
#
#     def __str__(self):
#         return str(self._sorted)
#
#     def __repr__(self):
#         return self.__str__()


