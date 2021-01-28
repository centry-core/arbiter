import logging

from uuid import uuid4
from time import sleep

from arbiter.base import Base

from arbiter.event.arbiter import ArbiterEventHandler
from arbiter.event.process import ProcessEventHandler
from arbiter.event.task import TaskEventHandler
from arbiter.event.broadcast import GlobalEventHandler
from arbiter.event.rpcServer import RPCEventHandler
from arbiter.event.rpcClient import RPCClintEventHandler
from arbiter.config import Config


class Minion(Base):
    def __init__(self, host, port, user, password, vhost="carrier", queue="default", all_queue="arbiterAll"):
        super().__init__(host, port, user, password, vhost, queue, all_queue)
        self.task_registry = {}
        self.task_handlers = []

    def apply(self, task_name, queue=None, tasks_count=1, task_args=None, task_kwargs=None, sync=True):
        task = Task(task_name, queue=queue if queue else self.config.queue,
                    tasks_count=tasks_count, task_args=task_args, task_kwargs=task_kwargs)
        for message in self.add_task(task, sync=sync):
            yield message

    def task(self, *args, **kwargs):
        """ Task decorator """
        def inner_task(func):
            def create_task(**kwargs):
                def _create_task(func):
                    return self._create_task_from_callable(func, **kwargs)
                return _create_task
            if callable(func):
                return create_task(**kwargs)(func)
            raise TypeError('@task decorated function must be callable')
        return inner_task

    def _create_task_from_callable(self, func, name=None, **kwargs):
        name = name if name else f"{func.__name__}.{func.__module__}"
        if name not in self.task_registry:
            self.task_registry[name] = func
        return self.task_registry[name]

    def rpc(self, workers, blocking=False):
        self.rpc = True
        state = dict()
        subscriptions = dict()
        logging.info("Starting '%s' RPC", self.config.queue)
        state["queue"] = self.config.queue
        for _ in range(workers):
            self.task_handlers.append(RPCEventHandler(self.config, subscriptions, state,
                                                      self.task_registry, wait_time=self.wait_time))
            self.task_handlers[-1].start()
            self.task_handlers[-1].wait_running()
        if blocking:
            for prcsrs in self.task_handlers:
                prcsrs.join()

    def run(self, workers):
        state = dict()
        subscriptions = dict()
        logging.info("Starting '%s' worker", self.config.queue)
        # Listen for task events
        state["queue"] = self.config.queue
        state["total_workers"] = workers
        state["active_workers"] = 0
        for _ in range(workers):
            TaskEventHandler(self.config, subscriptions, state, self.task_registry, wait_time=self.wait_time).start()
        # Listen for global events
        global_handler = GlobalEventHandler(self.config, subscriptions, state)
        global_handler.start()
        global_handler.join()


class Arbiter(Base):
    def __init__(self, host, port, user, password, vhost="carrier", all_queue="arbiterAll", start_consumer=True):
        super().__init__(host, port, user, password, vhost, all_queue=all_queue)
        self.arbiter_id = None
        self.state = dict(groups=dict())
        self.subscriptions = dict()
        self.handler = None
        if start_consumer:
            self.arbiter_id = str(uuid4())
            self.handler = ArbiterEventHandler(self.config, self.subscriptions, self.state, self.arbiter_id)
            self.handler.start()
            self.handler.wait_running()

    def apply(self, task_name, queue="default", tasks_count=1, task_args=None, task_kwargs=None, sync=False):
        task = Task(name=task_name, queue=queue, tasks_count=tasks_count,
                    task_args=task_args, task_kwargs=task_kwargs, callback_queue=self.arbiter_id)
        return list(self.add_task(task, sync=sync))

    def kill(self, task_key, sync=True):
        message = {
            "type": "stop_task",
            "task_key": task_key,
            "arbiter": self.arbiter_id
        }
        self.send_message(message, exchange=self.config.all)
        if sync:
            while True:
                if task_key in self.state and self.state[task_key]["state"] == "done":
                    break
                sleep(self.wait_time)

    def kill_group(self, group_id):
        tasks = []
        for task_id in self.state["groups"][group_id]:
            if task_id in self.state:
                tasks.append(task_id)
                self.kill(task_id, sync=False)
        tasks_done = []
        logging.info("Terminating ...")
        while not all(task in tasks_done for task in tasks):
            for task in tasks:
                if task not in tasks_done and self.state[task]["state"] == 'done':
                    tasks_done.append(task)
            sleep(self.wait_time)

    def status(self, task_key):
        if task_key in self.state:
            return self.state[task_key]
        elif task_key in self.state["groups"]:
            group_results = {
                "state": "done",
                "initiated": 0,
                "running": 0,
                "done": 0,
                "tasks": []
            }
            for task_id in self.state["groups"][task_key]:
                if task_id in self.state:
                    if self.state[task_id]["state"] in ["running", "initiated"]:
                        group_results["state"] = self.state[task_id]["state"]
                    group_results[self.state[task_id]["state"]] += 1
                    group_results["tasks"].append(self.state[task_id])
                else:
                    logging.info(f"[Group status] {task_id} is missing")
                    group_results["state"] = "running"
            return group_results
        else:
            raise NameError("Task or Group not found")

    def close(self):
        self.handler.stop()
        self._get_connection().queue_delete(queue=self.arbiter_id)
        self.handler.join()
        self.disconnect()

    def workers(self):
        message = {
            "type": "state",
            "arbiter": self.arbiter_id
        }
        if "state" in self.state:
            del self.state["state"]
        self.send_message(message, exchange=self.config.all)
        sleep(self.wait_time)
        return self.state["state"]

    def squad(self, tasks, callback=None):
        """
        Set of tasks that need to be executed together
        """
        workers_count = {}
        for each in tasks:
            if each.queue not in list(workers_count.keys()):
                workers_count[each.queue] = 0
            workers_count[each.queue] += each.tasks_count
        stats = self.workers()
        logging.info(f"Workers: {stats}")
        logging.info(f"Tests to run {workers_count}")
        for key in workers_count.keys():
            if not stats.get(key) or stats[key]["available"] < workers_count[key]:
                raise NameError(f"Not enough of {key} workers")
        return self.group(tasks, callback)

    def group(self, tasks, callback=None):
        """
        Set of tasks that need to be executed regardless of order
        """
        group_id = str(uuid4())
        self.state["groups"][group_id] = []
        tasks_array = []
        for each in tasks:
            task_id = str(uuid4())
            tasks_array.append(task_id)
            each.task_key = task_id
            each.callback_queue = self.arbiter_id
            if callback:
                each.callback = True
        for each in tasks:
            for task in self.add_task(each):
                self.state["groups"][group_id].append(task)
        if callback:
            callback.tasks_array = tasks_array
            callback.task_key = group_id
            callback.callback_queue = self.arbiter_id
            callback.task_type = "callback"
            for task in self.add_task(callback):
                self.state["groups"][group_id].append(task)
        return group_id

    def pipe(self, tasks, persistent_args=None, persistent_kwargs=None):
        """
        Set of tasks that need to be executed sequentially
        NOTE: Persistent args always before the task args
              Task itself need to have **kwargs if you want to ignore upstream results
        """
        pipe_id = str(uuid4())
        self.state["groups"][pipe_id] = []
        if not persistent_args:
            persistent_args = []
        if not persistent_kwargs:
            persistent_kwargs = {}
        res = {}
        yield {"pipe_id": pipe_id}
        for task in tasks:
            task.callback_queue = self.arbiter_id
            task.task_args = persistent_args + task.task_args
            for key, value in persistent_kwargs:
                if key not in task.task_kwargs:
                    task.task_kwargs[key] = value
            if res:
                task.task_kwargs['upstream'] = res.get("result")
            res = list(self.add_task(task, sync=True))
            self.state["groups"][pipe_id].append(res[0])
            res = res[1]
            yield res


class RPCClient(Base):
    def __init__(self, host, port, user, password, vhost="carrier", all_queue="arbiterAll"):
        super().__init__(host, port, user, password, vhost, all_queue=all_queue)
        self.subscriptions = {}
        self.handler = RPCClintEventHandler(self.config, self.subscriptions, self.state)
        self.handler.start()
        self.handler.wait_running()

    def call(self, tasks_module, task_name, *args, **kwargs):
        task_args = args if args else []
        task_kwargs = kwargs if kwargs else {}
        return self.handler.call(tasks_module, task_name, task_args, task_kwargs)

class Task:
    def __init__(self, name, queue='default', tasks_count=1, task_key="", task_type="task",
                 task_args=None, task_kwargs=None, callback=False, callback_queue=None):
        if not task_args:
            task_args = []
        if not task_kwargs:
            task_kwargs = {}
        self.task_type = task_type
        self.task_key = task_key
        self.name = name
        self.queue = queue
        self.tasks_count = tasks_count
        self.task_args = task_args
        self.task_kwargs = task_kwargs
        self.callback = callback
        self.callback_queue = callback_queue
        self.tasks_array = []  # this is for a task ids that need to be verified to be done before callback

    def to_json(self):
        return {
            "type": self.task_type,
            "queue": self.queue,
            "task_name": self.name,
            "task_key": self.task_key,
            "args": self.task_args,
            "kwargs": self.task_kwargs,
            "arbiter": self.callback_queue,
            "callback": self.callback,
            "tasks_array": self.tasks_array
        }
