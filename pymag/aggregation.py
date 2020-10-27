import pprint

from pymag.stages import AggStage
from pymag.typedlist import TypedList


class Aggregation(TypedList):
    """
    Execute a pymongo aggregation pipeline constructed from AggOperations
    """

    def __init__(self, *args):
        super().__init__(AggStage, args)

    @property
    def aggregation_string(self):
        return "[" + ", ".join([str(x) for x in self]) + "]"

def project_doc(doc, *args):
    if args:
        if args[0] in doc:
            return project_doc(doc[args[0]], *args[1:])
        else:
            return doc
    else:
        return doc


class CursorIterator(object):

    def __init__(self, cursor):
        self._cursor = cursor
        self._limit =1

    @property
    def limit(self):
        return self._limit

    @limit.setter
    def limit(self, limit):
        self._limit = limit

    def print(self, *args):
        if self._limit is None or self._limit == 0:
            for i in self._cursor:
                pprint.pprint(project_doc(i, *args))
        else:
            try:
                for _ in range(self._limit):
                    item = self._cursor.next()
                    pprint.pprint(project_doc(item, *args))
            except StopIteration:
                pass

