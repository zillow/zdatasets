from abc import ABC


class ProgramExecutor(ABC):
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
    def context(self) -> str:
        """
        execution context: offline, streaming, online
        """
        pass
