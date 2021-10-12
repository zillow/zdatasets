.. image:: https://travis-ci.org/zillow/zdatasets.svg?branch=master
    :target: https://travis-ci.org/zillow/zdatasets

.. image:: https://coveralls.io/repos/github/zillow/zdatasets/badge.svg?branch=master
    :target: https://coveralls.io/github/zillow/zdatasets?branch=master

.. image:: https://readthedocs.org/projects/zdatasets/badge/?version=latest
    :target: https://zdatasets.readthedocs.io/en/latest/?badge=latest
    :alt: Documentation Status


Welcome to datasets
==================================================

TODO

.. code-block:: python
    import pandas as pd
    from metaflow import FlowSpec, step

    from datasets.datasets_decorator import datasets
    from datasets.mode import Mode


    class HelloDatasetFlow(FlowSpec):
        @datasets.dataset(name="hello_dataset", partition_by="region", mode=Mode.Write)
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
