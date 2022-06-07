from enum import Flag, auto


class Context(Flag):
    """
    Used to express either the data or program execution context.
    """

    BATCH = auto()
    STREAMING = auto()
    ONLINE = auto()

    def __str__(self):
        return self.name

    __repr__ = __str__
