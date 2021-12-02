import pandas as pd
from metaflow import FlowSpec, step

from datasets import Mode, dataset
from datasets.txf_integration.txf_utils import (
    TXF_REGISTERED_DATASETS_ATTRIBUTE,
    TXF_CALLBACKS_ATTRIBUTE,
    TXF_METADATA_ATTRIBUTE,
    add_callback,
)


def udf():
    pass


class TxTestFlow(FlowSpec):
    @step
    def start(self):
        add_callback(self, "udf", udf)
        self.next(self.ds)

    @dataset(name="TxfDataset", partition_by="region", mode=Mode.WRITE)
    @step
    def ds(self):
        df = pd.DataFrame({"region": ["A", "A", "A", "B", "B", "B"], "home_id": [1, 2, 3, 4, 5, 6]})
        self.txf_dataset.write(df)
        assert hasattr(self, TXF_REGISTERED_DATASETS_ATTRIBUTE)
        assert hasattr(self, TXF_CALLBACKS_ATTRIBUTE)
        assert hasattr(self, TXF_METADATA_ATTRIBUTE)
        assert len(getattr(self, TXF_CALLBACKS_ATTRIBUTE)) == 1
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    TxTestFlow()
