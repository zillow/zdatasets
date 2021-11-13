from enum import Flag, auto


class Mode(Flag):
    """
    Used to represent data access modes.
    """

    READ = auto()
    WRITE = auto()
    READ_WRITE = READ | WRITE
