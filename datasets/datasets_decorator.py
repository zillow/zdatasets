import functools

from .dataset import Dataset


class datasets:
    @staticmethod
    def dataset(**dataset_kwargs):
        def step_decorator(func):
            @functools.wraps(func)
            def step_wrapper(*args, **kwargs):
                self = args[0]
                dataset = Dataset.from_keys(**dataset_kwargs)

                if not hasattr(self, dataset._attribute_name):
                    setattr(self, dataset._attribute_name, dataset)

                func(*args, **kwargs)

            return step_wrapper

        return step_decorator
