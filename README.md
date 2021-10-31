![Tests](https://github.com/zillow/datasets/actions/workflows/test.yml/badge.svg)
[![Coverage Status](https://coveralls.io/repos/github/zillow/datasets/badge.svg)](https://coveralls.io/github/zillow/datasets)
[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/zillow/datasets/tz/consistent_flow?urlpath=lab/tree/datasets/tutorials)

TODO: doc badge


Welcome to @datasets
==================================================

TODO

```python
import pandas as pd
from metaflow import FlowSpec, step

from datasets.datasets_decorator import datasets
from datasets.mode import Mode


class HelloDatasetFlow(FlowSpec):
    @datasets.dataset(name="hello_dataset", partition_by="region", mode=Mode.WRITE)
    @step
    def start(self):
        df = pd.DataFrame({"region": ["A", "A", "A", "B", "B", "B"], "zpid": [1, 2, 3, 4, 5, 6]})
        print("saving df: \n", df.to_string(index=False))

        # Example of writing to a dataset
        self.hello_dataset.write(df)

        self.next(self.end)

    @step
    def end(self):
        print(f"I have dataset \n{self.hello_dataset=}")

        # hello_dataset read_pandas()
        df: pd.DataFrame = self.hello_dataset.read_pandas()
        print("self.hello_dataset.read_pandas():\n", df.to_string(index=False))


if __name__ == "__main__":
    HelloDatasetFlow()
```
