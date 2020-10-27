import unittest
import os

from pymag import JSCode
from .simple_function import simple_function


def dummy_function(x: int, y: int, z: int):
    return [x, y, z]


class TestJSCode(unittest.TestCase):

    @staticmethod
    def error_function():
        pass

    def test_jscode(self):
        func_str = "function(x){print('{}({})'.format(simple_function.__name__,x));};"
        converter = JSCode(simple_function)
        self.assertEqual(func_str, converter.js_code_min)
        os.unlink(converter.hash_path)

    def test_Code(self):

        javascript_code = "function myFunction(p1, p2) {" \
                          "return p1 * p2;   // The function returns the" \
                          "}                 //product of p1 and p2"

        self.assertRaises(ValueError, JSCode, TestJSCode.error_function)
        x = JSCode(dummy_function, "bish", "bash", "bosh")
        print(x.js_code_min)
        os.unlink(x.hash_path)

if __name__ == '__main__':
    unittest.main()
