from pydantic import BaseModel
from datetime import datetime

class Task(BaseModel, frozen=True):
    task_id: str
    producer: str
    time_created: datetime
    subsystem: str
    work_type: str
    work_desc: str
    work_args: tuple
    work_kwargs: dict


class TaskStatus(BaseModel, frozen=True):
    pass