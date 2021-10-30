from enum import Flag, auto


class Mode(Flag):
    """
    Used to represent data access modes.
    """

    Read = auto()
    Write = auto()
