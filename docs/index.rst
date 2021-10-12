.. datasets documentation master file
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

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

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
