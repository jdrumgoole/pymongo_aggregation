import pprint

from pymongo_aggregation.aggoperation import AggOperation, DocOperation, ValueOperation


class Pipeline(list):
    """

    """

    def __init__(self, *args):
        super().__init__()
        self._agg_op = AggOperation()
        self._app_op_list = AggOperation.ops()

        for i in args:
            if isinstance(i, DocOperation) or isinstance(i, ValueOperation):
                self.append(i)
            else:
                raise ValueError("{} is not a class or sublcass of DocOperation or ValueOperation".format( i.name()))

    def __repr__(self):
        return "[" + ",\n ".join([repr(i) for i in self]) + "]"


    def pp(self):
        return pprint.pprint(self)

    def aggregate(self, collection):
        return collection.aggregate([i() for i in self])
