"""
Simple class to encapsulate MongoDB aggregation operations
Class name of sub-classes must match an AggObsolete operation.

TO create new agg operations just create a sub-class that
matches the class name. If you want to use a different name for
the class name just make sure to override the name operator with
the correct name for the MongoDB Aggregation operation
"""

import datetime
from collections import OrderedDict
import pprint

import pymongo


class AggOperation(object):
    """
    Super class for all Aggregation operations.
    Should not be instantiated directly. Use
    """

    subclasses = OrderedDict()

    @staticmethod
    def ops():
        return AggOperation.subclasses

    def name(self):
        return self.__class__.__name__

    def op_name(self):
        return "$" + self.name()

    def __repr__(self):
        return "{}".format(self.op_name())


class DocOperation(AggOperation):
    """
    Superclass for all MongoDB Aggregation operations that
    take a doc as a target field.

    e.g. { "$match" : { <match args }}
    """
    _doc: dict

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
        return {self.op_name(): self._doc}

    def __repr__(self):
        return pprint.pformat({self.op_name(): self._doc})


class ValueOperationError(ValueError):
    pass


class ValueOperation(AggOperation):
    """
    A value operation is an aggregation operation in which the
    argument is a bare value.

    e.g. { "$limit" : 30 } or { "$out" : "data_collection" }
    """

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        cls.subclasses[cls.__name__] = cls

    def __init__(self, value):
        self._value = value

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, value):
        self._value = value

    def __call__(self):
        return {self.op_name(): self._value }

    def __repr__(self):
        if type( self._value ) == str:
            v = "'{}'".format(self._value)
        else:
            v = "{}".format(self._value)

        return "{{'{}':{}}}".format(self.op_name(), v)

class match(DocOperation):
    pass


class range_match(match):
    """
    Create a match operator specifying that a field fall between two date
    ranges *start* and *end*.
    """

    def __init__(self, field, start=None, end=None):
        if start is not None and not isinstance(start, datetime.datetime):
            raise ValueError("{} is not and instance of datetime".format( start))
        if end is not None and not isinstance(end, datetime.datetime):
            raise ValueError("{} is not and instance of datetime".format( end ))

        self._doc = {field: {}}
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

        Range_Match is still a $match operator.
        :return: "match"

        """
        return "match"

class project(DocOperation):
    pass

class group(DocOperation):
    pass

class count(DocOperation):
    """
    Disambiguate from count function
    """

    def __init__(self, count_field):
        if isinstance( count_field, str):
            if count_field:
                self._doc = count_field
            else:
                raise ValueError("count_field cannot be None")
        else:
            raise ValueError("count_field must be a string (str type)")

    # def op_name(self):
    #     return "$count"

class lookup(DocOperation):
    pass

class sort(DocOperation):
    """
    Required for ordered sorting of fields as python dictionaries do not
    guarantee to maintain insertion order. Sorted fields are maintained
    in an ``OrderedDict`` class that ensures order is maintained.

    We can add sorts as named fields in the parameter lists or as tuples
    for field names that cannot be described using Python identifers.

    so

    sort(
    """

    def __init__(self, *args, **kwargs):
        """
        parameters are key="ascending" or key="descending"
        Can use kwargs in the form field=pymongo.ASCENDING because field may
        be a dotted string e.g. "group.name".
        """
        self._doc = OrderedDict()

        self.add(*args, **kwargs)

    def add(self, *args, **kwargs):
        """
        >>> sort("x")
        {'$sort': OrderedDict([('x', 1)])}
        >>> sort(x=1)
        {'$sort': OrderedDict([('x', 1)])}
        >>> sort(("x", 1))
        {'$sort': OrderedDict([('x', 1)])}
        >>> sort( x=pymongo.DESCENDING)
        {'$sort': OrderedDict([('x', -1)])}
        >>> sort( x=pymongo.ASCENDING)
        {'$sort': OrderedDict([('x', 1)])}

        >>> sort( x=2)
        Traceback (most recent call last):
            ...
        ValueError: 2 is not equal to pymongo.ASCENDING or pymongo.DESCENDING

        :param args: List of sort keys as strings or a list of tuples
        :param kwargs: List of sort keys and sort directions
        :return: obj
        """
        for i in args:
            if isinstance(i, tuple):
                self.add_sort(i[0], i[1])
            elif isinstance(i, str):
                self.add_sort(i)
            else:
                raise ValueError(f"{i} is not a tuple or string")

        for k,v in kwargs.items():
            if v in [ pymongo.ASCENDING, pymongo.DESCENDING]:
                self.add_sort(k, v)
            else:
                raise ValueError(f"{v} is not equal to pymongo.ASCENDING or pymongo.DESCENDING")

    def sort_fields(self):
        return self._doc.keys()

    def sort_items(self):
        return self._doc.items()

    def sort_directions(self):
        return self._doc.values()

    def add_sort(self, field, sort_order=pymongo.ASCENDING):
        if sort_order in [pymongo.ASCENDING, pymongo.DESCENDING]:
            self._doc[field] = sort_order
        else:
            raise ValueError("Invalid sort order must be pymongo.ASCENDING or pymongo.DESCENDING")


class sortByCount(DocOperation):
    pass


class unwind(DocOperation):
    pass


class redact(DocOperation):
    pass


# class count(ValueOperation):
#     pass


class limit(ValueOperation):

    def __init__(self, value):
        if type(value) != int:
            raise ValueError("{} requires a positive int argumemt (type of value is '{}".format(self.op_name(), type(value)))
        if value < 1 :
            raise ValueError("{} arguments must be 1 or more (value is: {})".format(self.op_name(), value))
        else:
            super().__init__(value)


class skip(ValueOperation):
    pass


class out(ValueOperation):
    pass


class addFields(DocOperation):
    pass


class bucket(DocOperation):
    pass


class bucketAuto(DocOperation):
    pass


class collStats(DocOperation):
    pass


class currentOp(DocOperation):
    pass


class facet(DocOperation):
    pass


class geoNear(DocOperation):
    pass


class graphLookup(DocOperation):
    pass


class indexStats(DocOperation):
    pass


class listLocalSessions(DocOperation):
    pass


class listSessions(DocOperation):
    pass


class replaceRoot(DocOperation):
    pass


class sample(DocOperation):
    pass


class Example_for_Sample_Op_with_name(DocOperation):

    def name(self):
        return "sample"


if __name__ == "__main__":
    import doctest
    doctest.testmod()
