import redis
from core.task_producing import TaskProducer
import time

if __name__ == "__main__":
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    prod = TaskProducer(r, "A", "test_prod")

    work = "std_job"
    prod.submit(work_type=work)

    while True:
        dct = prod.get_finished_tasks_and_results()
        if dct:
            print(dct)
        time.sleep(0.1)