from pymag import JSCode


class Operator:

    def __init__(self, doc: object):
        self._doc = doc

    def __call__(self):
        return {f"${self.__class__.__name__}": self._doc()}

class Accumulator:
    """
    https://docs.mongodb.com/manual/reference/operator/aggregation/accumulator/
    """

    def __init__(self,
                 init_code: JSCode,
                 accumulate_code: JSCode,
                 merge_code: JSCode,
                 finalize_code: JSCode):

        self._init_code = init_code
        self._accumulate_code = accumulate_code
        self._merge_code = merge_code
        self._finalize_code = finalize_code

    def __call__(self):
        pass


def abs(i):
    """
    https://docs.mongodb.com/manual/reference/operator/aggregation/abs
    """
    return f"{{'$abs' :{i}}}"


def accumulator(self):
    pass


class function(Operator):
    pass


if __name__ == "__main__":
    print(abs(-1))
