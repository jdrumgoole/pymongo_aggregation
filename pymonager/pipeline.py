import pprint

from .agg import DocOperation,ValueOperation


class Pipeline(list):
    """
    Execute a pymongo aggregation pipeline constructed from AggOperations
    """

    def __init__(self, collection, *args):
        super().__init__()
        self._collection = collection
        for i in args:
            if isinstance(i, DocOperation) or isinstance(i, ValueOperation):
                self.append(i)
            else:
                raise ValueError(f"{i} is not a class or sub class of DocOperation or ValueOperation")

    def __repr__(self):
        return "[" + ",\n ".join([repr(i) for i in self]) + "]"

    def collection(self):
        return self._collection

    def aggregate(self):
        return self._collection.aggregate([i() for i in self])


def project_doc(doc, *args):
    if args:
        if args[0] in doc:
            return project_doc( doc[args[0]], *args[1:] )
        else:
            return doc
    else:
        return doc

class CursorIterator(object):

    def __init__(self, cursor):
        self._cursor = cursor
        self._limit =1

    def set_limit(self,limit):
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


