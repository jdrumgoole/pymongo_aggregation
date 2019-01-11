import unittest
from pymonager.typedlist import TypedList


class TestTypedList(unittest.TestCase):

    def test_init(self):
        t = TypedList()
        self.assertTrue(isinstance(t, list))

        t = TypedList(int)
        self.assertRaises(ValueError, t.append, "hello")

    def test_overrides(self):

        t = TypedList(tuple)
        t = t + [(1,2)]
        self.assertEqual(t[0], (1, 2))


if __name__ == '__main__':
    unittest.main()
