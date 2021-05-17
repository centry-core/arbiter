from arbiter import Minion
from multiprocessing import Process
from time import sleep
import logging

app = Minion(host="localhost", port=5672, user='user', password='password', queue="default")


@app.task(name="add")
def add(x, y):
    logging.info("Running task 'add'")
    # task that initiate new task within same app
    increment = 0
    for message in app.apply('simple_add', task_args=[3, 4]):
        if isinstance(message, dict):
            increment = message["result"]
    logging.info("sleep done")
    return x + y + increment


@app.task(name="simple_add")
def adds(x, y):
    from time import sleep
    sleep(10)
    logging.info(f"Running task 'add_small' with params {x}, {y}")
    return x + y


@app.task(name="add_in_pipe")
def addp(x, y, upstream=0):
    logging.info("Running task 'add_in_pipe'")
    return x + y + upstream


@app.task(name="long_running")
def long_task():
    sleep(180)
    return "Long Task"


def run(rpc):
    if rpc:
        app.rpc(workers=1, blocking=True)
    else:
        app.run(workers=10)


def start_minion(rpc: bool = False) -> Process:
    p = Process(target=run, args=(rpc,))
    p.start()
    sleep(5)  # some time to start Minion
    return p


def stop_minion(p: Process):
    p.terminate()
    p.join()


if __name__ == "__main__":
    run(False)
