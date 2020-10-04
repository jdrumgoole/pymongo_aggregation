
class TypedList(list):

    def __init__(self, type_constraint=None, seq=()):
        self._type = type_constraint
        super().__init__([self._validate(x) for x in seq])

    def _validate(self, x):
        if self._type:
            if not isinstance(x, self._type):
                raise ValueError(f"{x} is not an instance of {self._type}")
            else:
                return x
        else:
            return x

    @property
    def item_type(self):
        return self._type

    def __add__(self, value):
        if self._validate(value):
            super().__add__(value)

    def __iadd__(self, value):
        if isinstance(value, TypedList):
            super().__iadd__(value)
        else:
            raise ValueError(f"{value} is not an instance of {self.__name__}")

    def __setitem__(self, key, value):
        if self._validate(value):
            super().__setitem__(key, value)

    def append(self, value):
        if self._validate(value):
            super().append(value)

    def extend(self, l):
        if type(l) == TypedList:
            if self.item_type == l.item_type:
                super().extend(l)
            else:
                raise ValueError(f"{l.item_type} does not match {self.item_type}")
        else:
            raise ValueError(f"{l} is not a TypedList")

    def insert(self, index, value):
        if self._validate(value):
            super().insert(index, value)


if __name__ == "__main__":

    l = TypedList(int, [])
    l.append(1)
