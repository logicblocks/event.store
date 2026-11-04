from typing import Any


def class_fullname(klass: type[Any]):
    """Get the fully qualified name of a class.

    Args:
        klass: The class to name.

    Returns:
        the module and qualified name joined by a dot, or just the qualified
            name for built-in classes.
    """
    module = klass.__module__
    if module == "builtins":
        return klass.__qualname__
    return module + "." + klass.__qualname__
