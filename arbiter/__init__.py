import logging
import pika

from uuid import uuid4
from json import dumps
from time import sleep

from arbiter.event.arbiter import ArbiterEventHandler
from arbiter.event.task import TaskEventHandler
from arbiter.event.broadcast import GlobalEventHandler
from arbiter.config import Config, task_types

connection = None


class Base:
    def __init__(self, host, port, user, password, vhost="carrier",
                 light_queue="arbiterLight", heavy_queue="arbiterHeavy", all_queue="arbiterAll"):
        self.config = Config(host, port, user, password, vhost, light_queue, heavy_queue, all_queue)
        self.state = dict()

    def _get_connection(self):
        global connection
        if not connection:
            _connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=self.config.host,
                    port=self.config.port,
                    virtual_host=self.config.vhost,
                    credentials=pika.PlainCredentials(
                        self.config.user,
                        self.config.password
                    )
                )
            )
            channel = _connection.channel()
            channel.queue_declare(
                queue=self.config.light, durable=True
            )
            channel.queue_declare(
                queue=self.config.heavy, durable=True
            )
            channel.exchange_declare(
                exchange=self.config.all,
                exchange_type="fanout", durable=True
            )
            connection = channel
        return connection

    def send_message(self, msg, queue="", exchange=""):
        self._get_connection().basic_publish(
            exchange=exchange, routing_key=queue,
            body=dumps(msg).encode("utf-8"),
            properties=pika.BasicProperties(
                delivery_mode=2
            )
        )

    def wait_for_tasks(self, tasks):
        tasks_done = []
        while not all(task in tasks_done for task in tasks):
            for task in tasks:
                if task not in tasks_done and self.state[task]["state"] == 'done':
                    tasks_done.append(task)
                    yield self.state[task]

    def add_task(self, task, sync=False):
        generated_queue = False
        if not task.callback_queue and sync:
            generated_queue = True
            queue_id = str(uuid4())
            self._get_connection().queue_declare(
                queue=queue_id, durable=True
            )
            task.callback_queue = queue_id
        tasks = []
        for _ in range(task.tasks_count):
            task_key = str(uuid4())
            tasks.append(task_key)
            if task.callback_queue:
                self.state[task_key] = {
                    "task_type": task.task_type,
                    "state": "initiated"
                }
            logging.info(task.to_json())
            message = task.to_json()
            message["task_key"] = task_key
            self.send_message(message, queue=self.config.__getattribute__(task.task_type))
            yield task_key
        if generated_queue:
            handler = ArbiterEventHandler(self.config, {}, self.state, task.callback_queue)
            handler.start()
        if sync:
            for message in self.wait_for_tasks(tasks):
                yield message
        if generated_queue:
            handler.stop()
            self._get_connection().queue_delete(queue=task.callback_queue)
            handler.join()


class Minion(Base):
    def __init__(self, host, port, user, password, vhost="carrier",
                 light_queue="arbiterLight", heavy_queue="arbiterHeavy", all_queue="arbiterAll"):
        super().__init__(host, port, user, password, vhost, light_queue,
                         heavy_queue, all_queue)
        self.task_registry = {}

    def apply(self, task_name, task_type="heavy", tasks_count=1, task_args=None, task_kwargs=None, sync=True):
        task = Task(task_name, task_type, tasks_count, task_args, task_kwargs)
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

    def _create_task_from_callable(self, func, name=None, task_type="heavy", **kwargs):
        name = name if name else f"{func.__name__}.{func.__module__}"
        if task_type not in task_types:
            raise TypeError(f'task_type is not supported please use any of [{",".join(task_types)}]')
        if name not in self.task_registry:
            self.task_registry[name] = func
        return self.task_registry[name]

    def run(self, worker_type, workers):
        state = dict()
        subscriptions = dict()
        self.config.worker_type = worker_type
        logging.info("Starting '%s' worker", self.config.worker_type)
        # Listen for task events
        state["type"] = worker_type
        state["total_workers"] = workers
        state["active_workers"] = 0
        for _ in range(workers):
            TaskEventHandler(self.config, subscriptions, state, self.task_registry).start()
        # Listen for global events
        global_handler = GlobalEventHandler(self.config, subscriptions, state)
        global_handler.start()
        global_handler.join()


class Arbiter(Base):
    def __init__(self, host, port, user, password, vhost="carrier", light_queue="arbiterLight",
                 heavy_queue="arbiterHeavy", all_queue="arbiterAll", start_consumer=True):
        super().__init__(host, port, user, password, vhost, light_queue,
                         heavy_queue, all_queue)
        self.arbiter_id = None
        self.state = dict(groups=dict())
        self.subscriptions = dict()
        self.handler = None
        if start_consumer:
            self.arbiter_id = str(uuid4())
            self.handler = ArbiterEventHandler(self.config, self.subscriptions, self.state, self.arbiter_id)
            self.handler.start()

    def apply(self, task_name, task_type="heavy", tasks_count=1, task_args=None, task_kwargs=None):
        task = Task(task_name, task_type, tasks_count, task_args, task_kwargs, callback_queue=self.arbiter_id)
        return list(self.add_task(task))

    def kill(self, task_key):
        message = {
            "type": "stop_task",
            "task_key": task_key,
            "arbiter": self.arbiter_id
        }
        self.send_message(message, exchange=self.config.all)
        while True:
            if self.state[task_key]["state"] == "done":
                break
            sleep(1)

    def status(self, task_key):
        if task_key in self.state:
            return self.state[task_key]["state"]
        elif task_key in self.state["groups"]:
            group_results = {
                "state": "done",
                "initiated": 0,
                "running": 0,
                "done": 0,
                "tasks": []
            }
            for task_id in self.state["groups"][task_key]:
                if self.state[task_id]["state"] in ["running", "initiated"]:
                    group_results["state"] = self.state[task_id]["state"]
                group_results[self.state[task_id]["state"]] += 1
                group_results["tasks"].append(self.state[task_id])
            return group_results
        else:
            raise NameError("Task or Group not found")

    def close(self):
        self.handler.stop()
        self._get_connection().queue_delete(queue=self.arbiter_id)
        self.handler.join()

    def workers(self):
        message = {
            "type": "state",
            "arbiter": self.arbiter_id
        }
        if "state" in self.state:
            del self.state["state"]
        self.send_message(message, exchange=self.config.all)
        sleep(2)
        return self.state["state"]

    def squad(self, tasks, callback=None):
        """
        Set of tasks that need to be executed together
        """
        workers_count = {"heavy": 0, "light": 0}
        for each in tasks:
            workers_count[each.task_type] += each.tasks_count
        stats = self.workers()
        for key in workers_count.keys():
            if workers_count[key] and stats[key]["available"] < workers_count[key]:
                raise NameError(f"Not enough of {key} workers")
        return self.group(tasks, callback)

    def group(self, tasks, callback=None):
        """
        Set of tasks that need to be executed regardless of order
        """
        group_id = str(uuid4())
        self.state["groups"][group_id] = []
        for each in tasks:
            each.callback_queue = self.arbiter_id
            for task in self.add_task(each):
                self.state["groups"][group_id].append(task)
        if callback:
            self.wait_for_tasks(self.state["groups"][group_id])
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


class Task:
    def __init__(self, name, task_type='heavy', tasks_count=1, task_args=None, task_kwargs=None, callback_queue=None):
        if not task_args:
            task_args = []
        if not task_kwargs:
            task_kwargs = {}
        self.name = name
        self.task_type = task_type
        self.tasks_count = tasks_count
        self.task_args = task_args
        self.task_kwargs = task_kwargs
        self.callback_queue = callback_queue

    def to_json(self):
        return {
            "type": "task",
            "task_name": self.name,
            "task_key": "",
            "args": self.task_args,
            "kwargs": self.task_kwargs,
            "arbiter": self.callback_queue
        }
