import pandas as pd
from metaflow import FlowSpec, step

import datasets
from datasets import Mode
from datasets.txf_integration.txf_utils import TXF_REGISTERED_DATASETS_ATTRIBUTE, TXF_CALLBACKS_ATTRIBUTE, \
    TXF_METADATA_ATTRIBUTE


class TxTestFlow(FlowSpec):

    @datasets.dataset(ds_name="txf_dataset", partition_by="region", mode=Mode.Write)
    @step
    def start(self):
        df = pd.DataFrame({"region": ["A", "A", "A", "B", "B", "B"], "home_id": [1, 2, 3, 4, 5, 6]})
        self.txf_dataset.write(df)
        assert hasattr(self, TXF_REGISTERED_DATASETS_ATTRIBUTE)
        assert hasattr(self, TXF_CALLBACKS_ATTRIBUTE)
        assert hasattr(self, TXF_METADATA_ATTRIBUTE)
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    TxTestFlow()
