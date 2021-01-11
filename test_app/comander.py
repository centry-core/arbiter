from time import sleep
from arbiter import Arbiter, Task

arbiter_host = "localhost"


def simple_task_in_task():
    arbiter = Arbiter(host=arbiter_host, port=5672, user='user', password='password')
    print(arbiter.workers())
    task_keys = arbiter.apply("add", task_args=[1, 2])
    for task_key in task_keys:
        print(arbiter.status(task_key))
    for message in arbiter.wait_for_tasks(task_keys):
        print(message)
    for task_key in task_keys:
        print(arbiter.status(task_key))
    arbiter.close()


def tasks_squad():
    arbiter = Arbiter(host=arbiter_host, port=5672, user='user', password='password')
    tasks = []
    for _ in range(3):
        tasks.append(Task("simple_add", task_args=[1, 2]))
    squad_id = arbiter.squad(tasks)
    while arbiter.status(squad_id).get("state") != "done":
        sleep(1)
    print(arbiter.status(squad_id))
    arbiter.close()


def tasks_group():
    arbiter = Arbiter(host=arbiter_host, port=5672, user='user', password='password')
    tasks = []
    for _ in range(20):
        tasks.append(Task("simple_add", task_args=[1, 2]))
    squad_id = arbiter.group(tasks)
    while arbiter.status(squad_id).get("state") != "done":
        sleep(1)
    print(arbiter.status(squad_id))
    arbiter.close()


def tasks_pipe():
    arbiter = Arbiter(host=arbiter_host, port=5672, user='user', password='password')
    tasks = []
    for _ in range(20):
        tasks.append(Task("add_in_pipe", task_args=[2]))
    pipe_id = None
    for message in arbiter.pipe(tasks, persistent_args=[2]):
        if "pipe_id" in message:
            pipe_id = message["pipe_id"]
        print(message)
    print(arbiter.status(pipe_id))
    arbiter.close()


if __name__ == "__main__":
    simple_task_in_task()
