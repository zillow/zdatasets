.. zdatasets documentation master file
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to zdatasets
==================================================

TODO

.. code-block:: python
    import pandas as pd
    from metaflow import FlowSpec, step

    from zdatasets.datasets_decorator import datasets
    from zdatasets.mode import Mode


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
            self.hello_dataset.write(df)

            self.next(self.end)

        @step
        def end(self):
            print(f"I have dataset \n{self.hello_dataset=}")

            # hello_dataset read_pandas()
            df: pd.DataFrame = self.hello_dataset.to_pandas()
            print("self.hello_dataset.to_pandas():\n", df.to_string(index=False))


    if __name__ == "__main__":
        HelloDatasetFlow()

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
