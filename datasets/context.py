from enum import Flag, auto


class Context(Flag):
    """
    Used to express either the data or program execution context.
    """

    Batch = auto()
    Streaming = auto()
    Online = auto()
