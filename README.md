![Tests](https://github.com/zillow/datasets/actions/workflows/test.yml/badge.svg)
[![Coverage Status](https://coveralls.io/repos/github/zillow/datasets/badge.svg)](https://coveralls.io/github/zillow/datasets)
[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/zillow/datasets/main?urlpath=lab/tree/datasets/tutorials)


Welcome to @datasets
==================================================

TODO

```python
import pandas as pd
from metaflow import FlowSpec, Parameter, current, step

from datasets import DatasetType, Mode


# Can also invoke from CLI:
#  > python datasets/tutorials/0_hello_dataset_flow.py run \
#    --hello_dataset '{"name": "foo", "partition_by": "region", "mode": "Write"}'
class HelloDatasetFlow(FlowSpec):
    hello_dataset = Parameter(
        "hello_dataset",
        default=dict(name="HelloDataset", partition_by="region", mode=Mode.Write),
        type=DatasetType,
    )

    @step
    def start(self):
        df = pd.DataFrame({"region": ["A", "A", "A", "B", "B", "B"], "zpid": [1, 2, 3, 4, 5, 6]})
        print("saving df: \n", df.to_string(index=False))

        # Example of writing to a dataset
        print(f"{self.hello_dataset.program_name=}")
        self.hello_dataset.write(df)

        self.next(self.end)

    @step
    def end(self):
        print(f"I have dataset \n{self.hello_dataset=}")

    # hello_dataset to_pandas()
    df: pd.DataFrame = self.hello_dataset.to_pandas(run_id=current.run_id)
    print("self.hello_dataset.to_pandas():\n", df.to_string(index=False))

    # save this as an output dataset
    self.output_dataset = self.hello_dataset


if __name__ == "__main__":
    HelloDatasetFlow()
```
