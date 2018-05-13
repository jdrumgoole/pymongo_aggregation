import pprint

from pymongo_aggregation.agg_operation import Agg_Operation, Doc_Operation, Value_Operation


class Pipeline(list):

    def __init__(self, *args ):
        super().__init__()
        self._agg_op = Agg_Operation()
        self._app_op_list = Agg_Operation.ops()

        for i in args:
            if isinstance( i, Doc_Operation ) or isinstance( i, Value_Operation):
                self.append( i )
            else:
                raise ValueError( "{} is not a class or sublcass of Doc_Operation or Value_Operation".format( i.name()))

    def __repr__(self):
        return "[" + ", ".join([repr(i) for i in self]) + "]"


    def pp(self):
        return pprint.pprint(self)

    def aggregate(self, collection):
        return collection.aggregate([ i() for i in self ])