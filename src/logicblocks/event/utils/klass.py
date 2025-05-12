from typing import Any


def class_fullname(klass: type[Any]):
    module = klass.__module__
    if module == "builtins":
        return klass.__qualname__
    return module + "." + klass.__qualname__
