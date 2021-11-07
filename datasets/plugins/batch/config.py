from datasets.config import from_conf
from datasets.data_container_type import DataContainerType


BATCH_DEFAULT_CONTAINER: DataContainerType = DataContainerType.from_str(
    from_conf("BATCH_DEFAULT_CONTAINER", DataContainerType.SPARK_PANDAS.value)
)
