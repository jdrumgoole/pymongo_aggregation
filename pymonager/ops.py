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


class AggOperation(dict):
    """
    Super class for all Aggregation operations.
    Should not be instantiated directly. Use
    DocOperation or ValueOperation
    """

    # __subclasses = OrderedDict()
    #
    # def __init_subclass__(cls, **kwargs):
    #     super().__init_subclass__(**kwargs)
    #     cls.__subclasses[cls.__name__] = cls

    def __init__(self, arg=None):
        if arg is None:
            self[self.op_name] = {}
        else:
            self[self.op_name] = arg

    # @staticmethod
    # def valid_op(op):
    #     """
    #     Every Sub class of AggOperations will register with subclasses
    #     then we can always determine the set of valid ops.
    #     :return: dictionary of valid subclasses
    #     """
    #     return op.name in AggOperation.__subclasses

    @property
    def arg(self):
        return self[self.op_name]

    @arg.setter
    def arg(self, arg):
        self[self.op_name] = arg

    @property
    def name(self):
        return self.__class__.__name__

    @property
    def op_name(self):
        return f"${self.name}"

    def __repr__(self):
        return f"{self.name}({self.arg!r})"

    def __str__(self):
        return f"{{'{self.op_name}': {self.arg}}}"


class DocOperation(AggOperation):
    """
    A DocOperation is a MongoDB aggregation operator that
    takes a doc as an argument.

    e.g. { "$match" : { <match args> }}
    """

    def __init__(self, arg=None):
        if arg is None:
            self[self.op_name] = {}
        elif isinstance(arg, dict):
            self[self.op_name] = arg
        else:
            raise ValueError(f'{arg} is not a dict object')

class ValueOperationError(ValueError):
    pass


class ValueOperation(AggOperation):
    """
    A value operation is an aggregation operation in which the
    argument is a bare value.

    e.g. { "$limit" : 30 } or { "$out" : "data_collection" }
    """

    def __init__(self, arg):
        if type(arg) in [ int, str, float, bool]:
            self[self.op_name] = arg
        else:
            raise ValueError(f"parameter arg to {self.__name__} ('{arg}') must be a simple type")


class match(DocOperation):

    @staticmethod
    def range_query(field, start=None, end=None):

        if start and end:
            doc = {field: {"$gte": start, "$lte": end}}
        elif start:
            doc = {field: {"$gte": start}}
        elif end:
            doc = {field: {"$lte": end}}

        return match(doc)

    @staticmethod
    def time_range_query(date_field, start=None, end=None):
        if start is not None and not isinstance(start, datetime.datetime):
            raise ValueError(f"{start} is not and instance of datetime")
        if end is not None and not isinstance(end, datetime.datetime):
            raise ValueError(f"{end} is not and instance of datetime")
        return match.range_query(date_field, start, end)

class project(DocOperation):
    pass

class group(DocOperation):
    pass

class count(ValueOperation):

    def __init__(self, count_field):
        if isinstance( count_field, str):
            if count_field:
                self._doc = count_field
            else:
                raise ValueError("count_field cannot be None")
        else:
            raise ValueError("count_field must be a string (str type)")

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

        self[self.op_name] = OrderedDict()

        self.add(*args, **kwargs)

    def add(self, *args, **kwargs):
        """
        >>> sort("x")
        {'$sort': OrderedDict([('x', 1)])}
        >>> sort(x=1)
        {'$sort': OrderedDict([('x', 1)])}
        >>> sort(("x", 1))
        {'$sort': OrderedDict([('x', 1)])}
        >>> sort(x=pymongo.DESCENDING)
        {'$sort': OrderedDict([('x', -1)])}
        >>> sort(x=pymongo.ASCENDING)
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
            if v in [pymongo.ASCENDING, pymongo.DESCENDING]:
                self.add_sort(k, v)
            else:
                raise ValueError(f"{v} is not equal to pymongo.ASCENDING or pymongo.DESCENDING")

    def sort_fields(self):
        return self[self.op_name].keys()

    def sort_items(self):
        return self[self.op_name].items()

    def sort_directions(self):
        return self[self.op_name].values()

    def add_sort(self, field, sort_order=pymongo.ASCENDING):
        if sort_order in [pymongo.ASCENDING, pymongo.DESCENDING]:
            self[self.op_name][field] = sort_order
        else:
            raise ValueError("Invalid sort order must be pymongo.ASCENDING or pymongo.DESCENDING")


class sortByCount(DocOperation):
    pass


class unwind(DocOperation):
    pass


class redact(DocOperation):
    pass


class limit(ValueOperation):

    def __init__(self, arg):
        if type(arg) != int:
            raise ValueError(f"{self.name} __init__requires an arg of type int (type of arg is '{type(arg)}')")
        elif arg < 1:
            raise ValueError(f"{self.name} __init__ parameters must be positive (arg={arg}")
        else:
            super().__init__(arg)


class skip(ValueOperation):
    pass


class out(ValueOperation):

    def __init__(self, arg):
        if isinstance(arg, str):
            if arg == "":
                raise ValueError( "collection names cannot be ''")
            if '$' in arg:
                raise ValueError("collection names cannot contain '$'")
            if arg.startswith("system."):
                raise ValueError("collection names cannot begin with 'system.'")
            self.arg = arg
        else:
            raise ValueError("parameter must be a string")


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

    @property
    def name(self):
        return "sample"


if __name__ == "__main__":
    import doctest
    doctest.testmod()
