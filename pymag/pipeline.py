
import pymag

import pprint


class Pipeline(list):

    def __init__(self, seq=()):
        super().__init__(seq)

        for i, d in enumerate(self):
            if not isinstance(d, pymag.AggStage):
                raise ValueError(f"the {i} element {d} is not an AggStage object")
            elif isinstance(d, pymag.out) and i < (len(self) -1):
                raise ValueError(f"$out must be last operator in the pipeline "
                                 f"it appears at position {i}")

    def __repr__(self):
        return "[" + ", ".join([repr(i) for i in self]) + "]"

    def __call__(self):
        return [i() for i in self]

    def __str__(self):
        return str([i() for i in self])

    def pp(self):
        return pprint.pprint(self)

    def aggregate(self, collection):
        return collection.aggregate(self())
