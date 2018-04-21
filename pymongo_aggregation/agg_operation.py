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
import pymongo

class Agg_Operation(object):
    """
    Superclass for all MongoDB Aggregation operations
    """

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

class project(Agg_Operation):
    pass

class group(Agg_Operation):
    pass

class count_x(Agg_Operation):
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

class lookup(Agg_Operation):
    pass

class sort(Agg_Operation):
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

class Sorter(object):
    '''
    Required for ordered sorting of fields as python dictionaries do not
    guarantee to maintain insertion order. Sorted fields are maintained
    in an ``OrderedDict`` class that ensures order is maintained.
    '''

    def __init__(self, **kwargs):
        '''
        parameters are key="asending" or key="descending"
        '''
        self._sorted = {}
        self._sorted["$sort"] = OrderedDict()

        self.add(kwargs)

    def add(self, sorts):
        for k, v in sorts.items():
            self.add_sort(k, v)

    def sort_fields(self):
        return self._sorted["$sort"].keys()

    def sort_items(self):
        return self._sorted["$sort"].items()

    def sort_directions(self):
        return self._sorted["$sort"].values()

    def sorts(self):
        return self._sorted

    def add_sort(self, field, sortOrder=pymongo.ASCENDING):
        if sortOrder in [pymongo.ASCENDING, pymongo.DESCENDING]:
            self._sorted["$sort"][field] = sortOrder
        else:
            raise ValueError("Invalid sort order must be pymongo.ASCENDING or pymongo.DESCENDING")

    def __call__(self):
        return self._sorted

    def __str__(self):
        return str(self._sorted)

    def __repr__(self):
        return self.__str__()


