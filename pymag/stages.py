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

import pymongo


class AggStage:
    """
    Super class for all Aggregation operations.
    Should not be instantiated directly. Use
    DocStage or ValueOperation
    """

    def __init__(self, arg=None):
        super().__init__()
        self._arg = arg
        self._name = self.__class__.__name__
        self._op_name = f"${self._name}"

    @property
    def arg(self):
        return self._arg

    @property
    def name(self):
        return self._name

    @property
    def op_name(self):
        return self._op_name

    def __call__(self):
        return {self._op_name: self._arg}

    def __repr__(self):
        return f"{self.name}({self.arg!r})"

    def __str__(self):
        return f"{{'{self.op_name}': {self.arg}}}"


class DocStage(AggStage):
    """
    A DocStage is a MongoDB aggregation operator that
    takes a doc as an argument.

    e.g. { "$match" : { <match args> }}
    """

    def __init__(self, arg=None):
        super().__init__(arg)
        if arg is None:
            self._arg = {}
        elif isinstance(arg, dict):
            self._arg = arg
        else:
            raise ValueError(f'{arg} is not a dict object')


class PositiveIntStage(AggStage):
    """
    A value operation is an aggregation operation in which the
    argument is positive int value.

    e.g. { "$limit" : 30 }
    """

    def __init__(self, arg):
        if type(arg) is not int:
            raise ValueError(f"arg {arg} to {self.__class__.__name__} must be an int")
        if arg < 0:
            raise ValueError(f"arg {arg} to {self.__class__.__name__} must be non-negative")

        super().__init__(arg)


class StrStage(AggStage):
    """
    A value operation is an aggregation operation in which the
    argument is a string value.

    e.g. { "$out" : "data_collection" }
    """

    def __init__(self, arg=""):
        super().__init__(arg)
        if type(arg) is not str:
            raise ValueError(f"parameter arg to {self.name}({arg}) must be a str")


class FloatStage(AggStage):
    """
    A value operation is an aggregation operation in which the
    argument is a float value.

    e.g. { "$limit" : 30 } or { "$out" : "data_collection" }
    """

    def __init__(self, arg):
        if type(arg) is not float:
            raise ValueError(f"parameter arg to {self.__name__} ('{arg}') must be a float")
        super().__init__(arg)


class BoolStage(AggStage):
    """
    A value operation is an aggregation operation in which the
    argument is a bool value.

    """

    def __init__(self, arg):
        if type(arg) is not bool:
            raise ValueError(f"parameter arg to {self.__name__} ('{arg}') must be a bool")
        super().__init__(arg)

    @staticmethod
    def time_range_query(date_field, start=None, end=None):
        if start is not None and not isinstance(start, datetime.datetime):
            raise ValueError(f"{start} is not and instance of datetime")
        if end is not None and not isinstance(end, datetime.datetime):
            raise ValueError(f"{end} is not and instance of datetime")
        return match.range_query(date_field, start, end)


class addFields(DocStage):
    """
    https://docs.mongodb.com/manual/reference/operator/aggregation/addFields/
    """
    pass


class bucket(DocStage):
    """
    https://docs.mongodb.com/manual/reference/operator/aggregation/bucket/
    """
    pass


class bucketAuto(DocStage):
    """
    https://docs.mongodb.com/manual/reference/operator/aggregation/bucketAuto/
    """
    pass


class collStats(DocStage):
    """
    https://docs.mongodb.com/manual/reference/operator/aggregation/collStats/
    """
    pass


class count(StrStage):
    """
    https://docs.mongodb.com/manual/reference/operator/aggregation/count/
    """
    pass


class facet(DocStage):
    """
    https://docs.mongodb.com/manual/reference/operator/aggregation/facet/
    """
    pass


class geoNear(DocStage):
    """
    https://docs.mongodb.com/manual/reference/operator/aggregation/geoNear/
    """
    pass


class graphLookup(DocStage):
    """
    https://docs.mongodb.com/manual/reference/operator/aggregation/graphLookup/
    """
    pass


class group(DocStage):
    """
    https://docs.mongodb.com/manual/reference/operator/aggregation/group/
    """
    pass


class indexStats(DocStage):
    """
    https://docs.mongodb.com/manual/reference/operator/aggregation/indexStats/
    """
    pass


class limit(PositiveIntStage):
    """
    https://docs.mongodb.com/manual/reference/operator/aggregation/limit/
    """
    pass


class listSessions(DocStage):
    """
    https://docs.mongodb.com/manual/reference/operator/aggregation/listSessions/
    """
    pass


class listLocalSessions(DocStage):
    """
    https://docs.mongodb.com/manual/reference/operator/aggregation/listLocalSessions/
    """
    pass


class lookup(DocStage):
    """
    https://docs.mongodb.com/manual/reference/operator/aggregation/lookup/
    """
    pass


class match(DocStage):
    """
    https://docs.mongodb.com/manual/reference/operator/aggregation/match/
    """
    @staticmethod
    def range_query(field, start=None, end=None):

        doc = {}
        if start and end:
            doc = {field: {"$gte": start, "$lte": end}}
        elif start:
            doc = {field: {"$gte": start}}
        elif end:
            doc = {field: {"$lte": end}}

        return match(doc)


class merge(DocStage):
    """
    https://docs.mongodb.com/manual/reference/operator/aggregation/merge/
    """
    pass


class out(StrStage):
    """
    https://docs.mongodb.com/manual/reference/operator/aggregation/out/
    """

    def __init__(self, arg):
        super().__init__(arg)
        if arg == "":
            raise ValueError("collection names cannot be any empty string")
        if '$' in arg:
            raise ValueError("collection names cannot contain '$'")
        if arg.startswith("system."):
            raise ValueError("collection names cannot begin with 'system.'")


class planCacheStats(DocStage):
    """
    https://docs.mongodb.com/manual/reference/operator/aggregation/planCacheStats/
    """
    pass


class project(DocStage):
    """
    https://docs.mongodb.com/manual/reference/operator/aggregation/project/
    """
    pass


class redact(DocStage):
    """
    https://docs.mongodb.com/manual/reference/operator/aggregation/redact/
    """
    pass


class replaceRoot(DocStage):
    """
    https://docs.mongodb.com/manual/reference/operator/aggregation/replaceRoot/
    """
    pass


class replaceWith(DocStage):
    """
    https://docs.mongodb.com/manual/reference/operator/aggregation/replaceWith/
    """
    pass


class sample(DocStage):
    """
    https://docs.mongodb.com/manual/reference/operator/aggregation/sample/
    """
    pass


class set(DocStage):
    """
    https://docs.mongodb.com/manual/reference/operator/aggregation/set/
    """
    pass


class skip(PositiveIntStage):
    """
    https://docs.mongodb.com/manual/reference/operator/aggregation/skip/
    """
    pass


class sort(DocStage):
    """
    https://docs.mongodb.com/manual/reference/operator/aggregation/sort/

    Required for ordered sorting of fields as python dictionaries do not
    guarantee to maintain insertion order. Sorted fields are maintained
    in an ``OrderedDict`` class that ensures order is maintained.

    We can add sorts as named fields in the parameter lists or as tuples
    for field names that cannot be described using Python identifiers.

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

        for k, v in kwargs.items():
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


class sortByCount(DocStage):
    """
    https://docs.mongodb.com/manual/reference/operator/aggregation/sortByCount/
    """
    pass


class unionWith(DocStage):
    """
    https://docs.mongodb.com/manual/reference/operator/aggregation/unionWith/
    """
    pass


class unset(DocStage):
    """
    https://docs.mongodb.com/manual/reference/operator/aggregation/unset/
    """
    pass
















class Example_for_Sample_Op_with_name(DocStage):

    def __init__(self, arg=None):
        super().__init__(arg)
        self._op_name = "$sample"

    @property
    def name(self):
        return "sample"


class unwind(DocStage):
    pass

if __name__ == "__main__":
    import doctest
    doctest.testmod()
