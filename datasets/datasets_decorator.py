import functools
import keyword
from typing import Callable, Dict, Optional, Union

from datasets._typing import ColumnNames
from datasets.dataset_plugin import Context, DatasetPlugin, StorageOptions
from datasets.mode import Mode
from datasets.utils.case_utils import pascal_to_snake_case


def dataset(
    name: Optional[str] = None,
    logical_key: Optional[str] = None,
    columns: Optional[ColumnNames] = None,
    run_id: Optional[str] = None,
    run_time: Optional[int] = None,
    mode: Union[Mode, str] = Mode.READ,
    options: Optional[StorageOptions] = None,
    options_by_context: Optional[Dict[Context, StorageOptions]] = None,
    context: Optional[Union[Context, str]] = None,
    field_name: Optional[str] = None,
    **dataset_kwargs,
):
    def step_decorator(func: Callable):
        @functools.wraps(func)
        def step_wrapper(*args, **kwargs):
            self = args[0]
            dataset_plugin: DatasetPlugin = DatasetPlugin.factory(
                name=name,
                logical_key=logical_key,
                columns=columns,
                run_id=run_id,
                run_time=run_time,
                mode=mode,
                options=options,
                options_by_context=options_by_context,
                context=context,
                **dataset_kwargs,
            )

            if field_name:
                if not field_name.isidentifier() or keyword.iskeyword(field_name):
                    raise ValueError(f"{field_name} is not a valid Python identifier")
                setattr(self, field_name, dataset_plugin)
            else:
                _snake_name = pascal_to_snake_case(dataset_plugin.name)
                setattr(self, _snake_name, dataset_plugin)

            func(*args, **kwargs)

        return step_wrapper

    return step_decorator
