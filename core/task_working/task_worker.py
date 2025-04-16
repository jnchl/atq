import redis
import time

from core.task_working.work_executor import WorkExecutor
from core.schemas import Task, WorkResult
from core.logger import get_logger

log = get_logger(__name__)


class TaskWorker:
    def __init__(self, redis_client: redis.Redis, subsystem: str, worker_name: str) -> None:
        self.redis_client = redis_client
        self.subsystem = subsystem
        self.worker_name = worker_name
        self.executors: dict[str, WorkExecutor] = {}
        self.worker_input_queue = f"{self.subsystem}:worker_input_queue:{self.worker_name}"
        self.worker_output_queue = f"{self.subsystem}:worker_output_queue:{self.worker_name}"
        self.result_hash = f"{self.subsystem}:result_hash"

    def attach_executor(self, work_executor: WorkExecutor):
        """Attaches executor to this worker.

        Args:
            work_executor (WorkExecutor): Class that derives from WorkExecutor which will do the actual work
        """
        work_type = work_executor.work_type

        if work_type in self.executors.keys():
            log.info(f"Already got executor for work type: {work_type}, overwriting")
        self.executors[work_type] = work_executor

    def run(self):
        """Starts worker"""
        work_types = list(self.executors.keys())
        work_queues = [f"{self.subsystem}:work_queue:{work_type}" for work_type in work_types]
        log.info(f"Worker: {self.worker_name} started running and looks at: {str(work_queues)}")

        while True:
            # if manager hasn't picked up finished task dont do anything
            if self.redis_client.llen(self.worker_output_queue) > 0:
                continue

            # if there's nothing on the input queue look for work
            if self.redis_client.llen(self.worker_input_queue) == 0:
                for work_queue in work_queues:
                    task_str: str | None = self.redis_client.lmove(work_queue, self.worker_input_queue, "RIGHT", "LEFT")
                    if task_str is not None:
                        task = Task.model_validate_json(task_str)
                        log.info(f"Fetched task with work type: {task.work_type} with id: {task.task_id} from {work_queue} and placed into {self.worker_input_queue}")
                        break

            # if there is something on input queue, process it and move it to output queue
            if self.redis_client.llen(self.worker_input_queue) > 0:
                task_str = self.redis_client.lrange(self.worker_input_queue, -1, -1)[0]
                task = Task.model_validate_json(task_str)
                log.info(f"Processing task with work type: {task.work_type} with id: {task.task_id}")
                work_type = task.work_type
                executor = self.executors[work_type]
                try:
                    work_result = executor.execute(
                        work_desc=task.work_desc, 
                        work_args=task.work_args if task.work_args else (), 
                        work_kwargs=task.work_kwargs if task.work_kwargs else {},
                        )
                except Exception as e:
                    work_result = WorkResult(error=str(e), output="")

                self.redis_client.hset(self.result_hash, task.task_id, work_result.model_dump_json())
                self.redis_client.lmove(self.worker_input_queue, self.worker_output_queue, "RIGHT", "LEFT")
                log.info(f"Worker: {self.worker_name} sent processed task with work {task.work_type} with id: {task.task_id} to queue: {self.worker_output_queue}")
            time.sleep(0.1)    



    