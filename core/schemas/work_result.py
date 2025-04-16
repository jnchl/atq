from pydantic import BaseModel


class WorkResult(BaseModel, frozen=True):
    error: str
    output: str

