from arbiter import Minion
import logging
from time import sleep

app = Minion(host="localhost", port=5672, user='user', password='password', queue="rcp_queue")


@app.task(name="add")
def add(x, y):
    logging.info("Running task 'add'")
    sleep(10)
    # task that initiate new task within same app
    for message in app.apply('simple_add', task_args=[3, 4]):
        logging.info(message)
    logging.info("sleep done")
    return x+y


@app.task(name="simple_add")
def adds(x, y):
    logging.info(f"Running task 'add_small' with params {x}, {y}")
    return x + y


@app.task(name="add_in_pipe")
def addp(x, y, upstream=0):
    logging.info("Running task 'add_in_pipe'")
    return x + y + upstream


if __name__ == "__main__":
    # app.run(worker_type="heavy", workers=3)
    app.rpc(workers=3, blocking=True)
