from pymag.ops import AggOperation
import pprint


class Pipeline(list):

    def __init__(self, *args, **argv):
        super().__init__()
        self._agg_op = AggOperation()
        self._app_op_list = AggOperation.ops()

        for i in args:
            if i.class_name() in AggOperation.ops():
                if i.class_name() in self._app_op_list:
                    self.append(i)
                else:
                    raise ValueError("{} is not a valid operation".format(i.class_name()))
            else:
                raise ValueError("{} is not a subclass of {}".format(i.class_name(), self._agg_op.__name__))

        for (k, v) in argv.items():
            if k in AggOperation.ops():
                self.append(AggOperation.ops()[str(k)](v))
            else:
                raise ValueError("{} is not a subclass of {}".format(k, self._agg_op.__name__))

    def __repr__(self):
        return "[" + ", ".join([repr(i) for i in self]) + "]"

    def __call__(self):
        return [i() for i in self]

    def pp(self):
        return pprint.pprint(self)

    def aggregate(self, collection):
        return collection.aggregate(self())
