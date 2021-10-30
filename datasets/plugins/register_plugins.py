from datasets.dataset_plugin import DatasetPlugin
from datasets.plugins import MetaflowExecutor


def register():
    from importlib_metadata import entry_points

    # Register plugins
    for entry in entry_points(group="datasets.plugins"):
        entry.load()

    # Register default executor first
    DatasetPlugin.register_executor(executor=MetaflowExecutor())

    for entry in entry_points(group="datasets.executors"):
        executor = entry.load()
        if not isinstance(executor, type(MetaflowExecutor)):
            DatasetPlugin.register_executor(executor=executor)
