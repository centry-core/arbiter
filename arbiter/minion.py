#   Copyright 2020 getcarrier.io
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.


import logging

from .base import Base
from .event.task import TaskEventHandler
from .event.broadcast import GlobalEventHandler
from .event.rpcServer import RPCEventHandler
from .task import Task


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