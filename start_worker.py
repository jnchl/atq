import redis
from datetime import datetime
import time
from core.schemas import WorkResult
from core.task_working import TaskWorker, WorkExecutor

class StdJobExecutor(WorkExecutor):
    def __init__(self) -> None:
        super().__init__(work_type="std_job")

    def execute(self, work_desc: str, work_args: tuple, work_kwargs: dict) -> WorkResult:
        time.sleep(5)
        result = WorkResult(error="nie ma", output=f"zadanie wykonane o {str(datetime.now())}")
        return result
        
 
if __name__ == "__main__":
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    tw = TaskWorker(r, "A", "test_worker")
    tw.attach_executor(work_executor=StdJobExecutor())
    tw.run()