import redis
from core.task_managing import TaskManager

if __name__ == "__main__":
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    tm = TaskManager(r, ["A"], "test_manager")
    tm.run()