from enum import Flag, auto


class Mode(Flag):
    """
    Used to represent data access modes.
    """

    READ = auto()
    WRITE = auto()
    READ_WRITE = READ | WRITE

    def __str__(self):
        return self.name

    __repr__ = __str__

    def to_json(self):
        return str(self)
