from pymongo_aggregation.agg_operation import Agg_Operation
import pprint

class Pipeline(list):

    def __init__(self, *args, **argv):
        super().__init__()
        self._agg_op = Agg_Operation()
        self._app_op_list = Agg_Operation.ops()

        for i in args:
            if i.name() in Agg_Operation.ops():
                if i.name() in self._app_op_list:
                    self.append( i )
                else:
                    raise ValueError( "{} is not a valid operation".format( i.name()))
            else:
                raise ValueError( "{} is not a subclass of {}".format( i.name(), self._agg_op.__name__))

        for (k,v) in argv.items():
            if k in Agg_Operation.ops():
                self.append( Agg_Operation.ops()[str(k)](v))
            else:
                raise ValueError( "{} is not a subclass of {}".format( k,self._agg_op.__name__ ))

    def __repr__(self):
        return "[" + ", ".join([repr(i) for i in self]) + "]"

    def __call__(self):
        return [ i() for i in self ]

    def pp(self):
        return pprint.pprint( self)

    def aggregate(self, collection):
        return collection.aggregate(self())