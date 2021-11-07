import os
from typing import Optional, TypeVar


_T = TypeVar("_T")


def from_conf(name: str, default: Optional[_T] = None) -> Optional[_T]:
    # TODO: should we have a default datasets config file?
    return os.environ.get(name, default)
