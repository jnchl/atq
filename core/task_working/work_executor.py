from abc import ABC, abstractmethod
from core.schemas import WorkResult

class WorkExecutor(ABC):
    def __init__(self, work_type: str) -> None:
        self.work_type = work_type

    @abstractmethod
    def execute(self, work_desc:str, work_args:tuple, work_kwargs: dict) -> WorkResult:
        raise NotImplementedError