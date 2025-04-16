import redis
from datetime import datetime
import uuid

from core.schemas import Task, WorkResult
from core.logger import get_logger

log = get_logger(__name__)

class TaskProducer:
    def __init__(self, redis_client: redis.Redis, subsystem:str, prod_name:str) -> None:
        """Class for scheduling tasks.

        Args:
            redis_client (redis.Redis): redis client
            subsystem (str): name of the subsystem to use 
            prod_name (str): name of this producer
        """
        self.redis_client = redis_client
        self.subsystem = subsystem
        self.prod_name = prod_name
        self.sent_not_finished:dict[str, dict[str, Task]] = {}
        self.prod_queue = f"{subsystem}:prod_queue"
        self.producer_finished_queue = f"{subsystem}:producer_finished_queue:{self.prod_name}"
        self.result_hash = f"{self.subsystem}:result_hash"

    def submit(self, work_type:str, work_desc:str = "", work_args:tuple | None = None, work_kwargs: dict | None = None) -> str:
        """Submits work to system.

        Args:
            work_type (str): which type of worker to use
            work_desc (str, optional): optional description. Defaults to "".
            work_args (tuple | None, optional): arguments for final function. Defaults to None.
            work_kwargs (dict | None, optional): keyword arguments for final function. Defaults to None.

        Returns:
            str: id of generated task
        """
        if work_args is None:
            work_args = ()

        if work_kwargs is None:
            work_kwargs = {}

        task_id = str(uuid.uuid4())
        task = Task(
            task_id=task_id,
            producer=self.prod_name, 
            time_created=datetime.now(),
            subsystem=self.subsystem,
            work_type=work_type,
            work_desc=work_desc,
            work_args=work_args,
            work_kwargs=work_kwargs)
        
        log.info(f"Sending task with work: {work_type} with id: {task_id}")
        self.redis_client.lpush(self.prod_queue, task.model_dump_json())
        self._add_to_sent_not_finished(task)
        return task_id
    
    def get_finished_tasks_and_results(self) -> dict[str, WorkResult]:
        """Gets all finished tasks with results.

        Returns:
            dict[str, WorkResult]: dict with task id's and their results
        """
        tasks_and_result = {}
        while True:
            task_str: str | None = self.redis_client.rpop(self.producer_finished_queue)
            if task_str is None:
                return tasks_and_result
            task = Task.model_validate_json(task_str)
            result = WorkResult.model_validate_json(self.redis_client.hget(self.result_hash, task.task_id))
            tasks_and_result[task.task_id] = (task, result)
            

    def _add_to_sent_not_finished(self, task: Task) -> None:
        task_id = task.task_id
        work_type = task.work_type

        if work_type in self.sent_not_finished.keys():
            self.sent_not_finished[work_type][task_id] = task
        else:
            self.sent_not_finished[work_type] = {task_id: task} 
    
