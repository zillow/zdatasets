import functools

from .dataset import Dataset
from .mode import Mode


class datasets:
    @staticmethod
    def dataset(
        name: str = None,
        flow_dataset: str = None,
        key: str = None,
        partition_by: str = None,
        path: str = None,
        columns=None,
        mode: Mode = Mode.Read,
    ):
        def step_decorator(func):
            @functools.wraps(func)
            def step_wrapper(*args, **kwargs):
                self = args[0]
                dataset_name = name
                if name is None and flow_dataset:
                    flow_name, dataset_name = flow_dataset.split(".")

                if not hasattr(self, dataset_name):
                    dataset = Dataset(
                        name=dataset_name,
                        flow_dataset=flow_dataset,
                        key=key,
                        partition_by=partition_by,
                        path=path,
                        columns=columns,
                        mode=mode,
                    )
                    setattr(self, dataset_name, dataset)

                func(*args, **kwargs)

            return step_wrapper

        return step_decorator
