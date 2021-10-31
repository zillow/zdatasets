from abc import ABC

from datasets.context import Context


class ProgramExecutor(ABC):
    """
    Class to access information about the ML program currently being executed.
    """

    @property
    def current_run_id(self) -> str:
        pass

    @property
    def datastore_path(self) -> str:
        pass

    @property
    def current_program_name(self) -> str:
        pass

    @property
    def context(self) -> Context:
        """
        The current default data context for this execution environment.
        """
        pass
