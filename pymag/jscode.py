"""
Represent code fragments in used in $function and other places.
"""
from datetime import datetime
import hashlib
import os
import inspect


class JSCode:
    """
    Generate JavaScript code from a python function to allow Python to
    be used when writing $function operators in the MongoDB Aggregation
    Framework.

    https://docs.mongodb.com/manual/reference/operator/aggregation/function/

    Uses the transcrypt package.

    https://pypi.org/project/Transcrypt/

    """

    TARGET_DIR = "__target__"

    @staticmethod
    def add_dollar(s):
        """
        Add a preceding dollar ($) to a string unless one exists already
        """
        if s is None:
            raise ValueError("Cannot add a $ sign to None")

        assert s[0] not in ["'", "\""]

        if s.startswith("$"):
            return s
        else:
            return f"${s}"

    @staticmethod
    def remove_dollar(s):
        if s is None:
            raise ValueError("Cannot add a $ sign to None")

        assert s[0] not in ["'", "\""]

        if s.startswith("$"):
            return s[1:]
        else:
            return s

    def __init__(self, python_func, *args):

        self._js_code = None
        self._python_func = python_func
        assert isinstance(self._python_func, object)
        self._python_code = inspect.getsource(self._python_func)
        self._args = args
        self._js_args = []
        for i in self._args:
            assert isinstance(i, str)
            self._js_args.append(self.add_dollar(i))

        hash_str = hashlib.sha256()
        hash_str.update(self._python_code.encode("utf-8"))
        self._hash = hash_str.hexdigest()
        self._hash_path = str(self._hash)+".py"

        if "." in self._python_func.__qualname__:
            raise ValueError(f"You can only encode top level functions: "
                             f"{self._python_func.__qualname__} is a nested function")

        if os.path.exists(self._hash_path):
            self._js_code = JSCode.to_js(self._hash_path, self._python_func)
        else:
            with open(f"{self._hash_path}", "w") as hash_file:
                hash_file.write(f"# qualified name : {self._python_func.__qualname__}\n")
                hash_file.write(f"# UTC Timestamp  : {datetime.utcnow()}\n")
                hash_file.write(self._python_code)
            self._js_code = JSCode.to_js(f"{self._hash_path}", self._python_func)

    @property
    def hash_path(self):
        return self._hash_path

    @property
    def lang(self):
        return "js"

    def __call__(self):
        return {"body": self.js_code_min,
                "args": self._js_args,
                "lang": self.lang}

    def __repr__(self):
        return f"{self.__class__.__name__}({self._python_func, self._args})"

    transcrypt_cmd = "transcrypt -n -u .auto -p .none -xt"

    @staticmethod
    def to_js(input_filename, f):

        filename_base = os.path.basename(input_filename)
        filename_root, _ = os.path.splitext(filename_base)
        js_filename = os.path.join(JSCode.TARGET_DIR, f"{filename_root}.js")

        if not os.path.isfile(input_filename):
            raise OSError(f"No such file:{input_filename}")
        else:
            print(f"{JSCode.transcrypt_cmd} {input_filename}")
            os.system(f"{JSCode.transcrypt_cmd} {input_filename}")
            js_code = JSCode.extract_js(js_filename, f)
        if len(js_code) == 0:
            raise ValueError(f"No function called {f.__name__}extracted from {js_filename}")

        return js_code

    @property
    def js_code(self):
        return '\n'.join(self._js_code)

    @property
    def js_code_min(self):
        return''.join([x.strip() for x in self._js_code])

    @staticmethod
    def extract_js(filename, f):

        func = []
        count = 1
        with open(filename, "r") as input_file:
            for count, line in enumerate(input_file, 1):
                if len(func) > 0:
                    # we are now parsing the function
                    func.append(line.rstrip())
                    if line.startswith("};"):  # parsing is over
                        return func
                else:
                    line = line.lstrip()
                if line.startswith("export"):
                    definition, equals, funcdef = line.partition("=")
                    definition = definition.strip()
                    func.append(funcdef.rstrip())
                    if equals != "=":
                        raise ValueError(f"Parsing of {filename} failed at line {count}"
                                         f": No equals sign (=) found")
                    if not definition.startswith("export var"):
                        raise ValueError(f"Parsing of {filename} failed at line {count}"
                                         f" 'exports var' not found"
                                         )
                    if not definition.endswith(f.__name__):
                        raise ValueError(f"Parsing of {filename} failed at line {count}"
                                         f"function definition is {definition} not {function_name}")
