![Tests](https://github.com/zillow/datasets/actions/workflows/test.yml/badge.svg)
[![Coverage Status](https://coveralls.io/repos/github/zillow/datasets/badge.svg)](https://coveralls.io/github/zillow/datasets)
[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/zillow/datasets/main?urlpath=lab/tree/datasets/tutorials)


# Welcome to zdatasets

## Development
* Set the version to a dev version, e.g. `1.3.0.dev1` in `pyproject.toml` when starting development.
* Bump the dev version (e.g., 1.3.0.dev1 → 1.3.0.dev2) every time you have a change you want to test in other repositories.
* After every change, confirm that the github workflow runs are successful at https://github.com/zillow/zdatasets/actions.
* The dev versions are published in test PyPI at https://test.pypi.org/project/zdatasets/#history.
* While testing your changes, you may need to reference your merge request in other repositories' `pyproject.toml` instead of using the dev version. For example, 
```
dataset = [
  "zdatasets[kubernetes] @ git+https://github.com/zillow/zdatasets.git@refs/pull/42/head"
]
```
* Bump the release version (e.g., 1.3.0.dev2 → 1.3.1) before merging your code change.
* Confirm the release of the new version in PyPI at https://pypi.org/project/zdatasets/#history.
* Create the release in https://github.com/zillow/zdatasets/releases.
* For any authentication issues in publishing to PyPI, ask for help in [the #open-source slack channel](https://zillowgroup.enterprise.slack.com/archives/C4NC77QG4).


## Example
```python
import pandas as pd
from metaflow import FlowSpec, step

from zdatasets import Dataset, Mode
from zdatasets.metaflow import DatasetParameter
from zdatasets.plugins import BatchOptions


# Can also invoke from CLI:
#  > python zdatasets/tutorials/0_hello_dataset_flow.py run \
#    --hello_dataset '{"name": "HelloDataset", "mode": "READ_WRITE", \
#    "options": {"type": "BatchOptions", "partition_by": "region"}}'
class HelloDatasetFlow(FlowSpec):
    hello_dataset = DatasetParameter(
        "hello_dataset",
        default=Dataset("HelloDataset", mode=Mode.READ_WRITE, options=BatchOptions(partition_by="region")),
    )

    @step
    def start(self):
        df = pd.DataFrame({"region": ["A", "A", "A", "B", "B", "B"], "zpid": [1, 2, 3, 4, 5, 6]})
        print("saving data_frame: \n", df.to_string(index=False))

        # Example of writing to a dataset
        self.hello_dataset.write(df)

        # save this as an output dataset
        self.output_dataset = self.hello_dataset

        self.next(self.end)

    @step
    def end(self):
        print(f"I have dataset \n{self.output_dataset=}")

        # output_dataset to_pandas(partitions=dict(region="A")) only
        df: pd.DataFrame = self.output_dataset.to_pandas(partitions=dict(region="A"))
        print('self.output_dataset.to_pandas(partitions=dict(region="A")):')
        print(df.to_string(index=False))


if __name__ == "__main__":
    HelloDatasetFlow()

```
