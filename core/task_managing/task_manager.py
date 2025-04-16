import redis
from datetime import datetime
import time

from core.schemas import Task
from core.logger import get_logger

log = get_logger(__name__)

class TaskManager:
    def __init__(self, redis_client: redis.Redis, subsystems:list[str], manager_name:str) -> None:
        self.redis_client = redis_client
        self.subsystems = subsystems
        self.manager_name = manager_name
        self.manager_to_worker_queue = f"manager:manager_to_worker_queue:{manager_name}"
        self.manager_from_worker_queue = f"manager:manager_from_worker_queue:{manager_name}"
        log.info(f"created TaskManager with name: {manager_name}")

    def run(self):
        log.info(f"Task manager: {self.manager_name} running, looking at subsystems: {self.subsystems}")
        while True:
            for subsystem in self.subsystems:
                prod_queue = f"{subsystem}:prod_queue"

                # move task from prod_queue onto manager_to_worker_queue (itself)
                task_str: str | None = self.redis_client.lmove(prod_queue, self.manager_to_worker_queue, "RIGHT", "LEFT")
                if task_str is not None:
                    task = Task.model_validate_json(task_str)
                    log.info(f"Fetched task with work type: {task.work_type} with id: {task.task_id} from {prod_queue} and placed into {self.manager_to_worker_queue}")

                # move task from manager_to_worker_queue to right work queue
                while self.redis_client.llen(self.manager_to_worker_queue) > 0:
                    in_task_str: str = self.redis_client.lrange(self.manager_to_worker_queue, -1, -1)[0]
                    in_task = Task.model_validate_json(in_task_str)
                    work_type = in_task.work_type
                    work_queue = f"{subsystem}:work_queue:{work_type}"        
                    self.redis_client.lmove(self.manager_to_worker_queue, work_queue, "RIGHT", "LEFT")
                    log.info(f"Got task with work type: {work_type} with id: {in_task.task_id}, sent to queue: {work_queue}")

                # check if any worker finished any task and move it to manager_from_worker_queue
                cursor, worker_output_queues = self.redis_client.scan(match=f"{subsystem}:worker_output_queue:*")
                for worker_output_queue in worker_output_queues:
                    task_str: str | None = self.redis_client.lmove(worker_output_queue, self.manager_from_worker_queue, "RIGHT", "LEFT")
                    if task_str is not None:
                        task = Task.model_validate_json(task_str)
                        log.info(f"Fetched task with work type: {task.work_type} with id: {task.task_id} from {worker_output_queue} and placed into {self.manager_from_worker_queue}")

                # check if there is something on manager_from_worker_queue
                while self.redis_client.llen(self.manager_from_worker_queue) > 0:
                    out_task_str = self.redis_client.lrange(self.manager_from_worker_queue, -1, -1)[0]
                    out_task = Task.model_validate_json(out_task_str)
                    out_task_producer_queue = f"{out_task.subsystem}:producer_finished_queue:{out_task.producer}"
                    self.redis_client.lmove(self.manager_from_worker_queue, out_task_producer_queue, "RIGHT", "LEFT")
                    log.info(f"Got finished task with work_type: {out_task.work_type} with id: {out_task.task_id}, moved to queue: {out_task_producer_queue}")
            time.sleep(0.1)

                




