from enum import Enum


class NoValue(Enum):
    def __repr__(self):
        return "<%s.%s>" % (self.__class__.__name__, self.name)


class DataContainerType(NoValue):
    SPARK_PANDAS = "SPARK_PANDAS"
    PANDAS = "PANDAS"
    SPARK_PANDAS_OR_PANDAS = "SPARK_PANDAS_OR_PANDAS"
    DASK = "DASK"
    PYTHON = "PYTHON"  # ex: list[dict]

    @classmethod
    def from_str(cls, input_str: str):
        for v in cls:
            if v.value.lower() == input_str.lower():
                return v
        raise ValueError(f"{cls.__name__} has no value matching {input_str}")
