from abc import ABC


class ProgramExecutor(ABC):
    @property
    def run_id(self) -> str:
        pass

    @property
    def datastore_path(self) -> str:
        pass

    @property
    def program_name(self) -> str:
        pass

    @property
    def context(self) -> str:
        pass
