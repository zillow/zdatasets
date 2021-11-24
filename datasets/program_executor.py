from abc import ABC, abstractmethod

from datasets.context import Context


class ProgramExecutor(ABC):
    """
    Class to access information about the program currently being executed.
    """

    @property
    @abstractmethod
    def current_run_id(self) -> str:
        pass

    @property
    @abstractmethod
    def datastore_path(self) -> str:
        pass

    @property
    @abstractmethod
    def current_program_name(self) -> str:
        pass

    @property
    @abstractmethod
    def context(self) -> Context:
        """
        The current default data context for this execution environment.
        """
        pass
