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
from uuid import uuid4
from time import sleep

from .base import Base
from .event.arbiter import ArbiterEventHandler
from .task import Task


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
            if each.task_type != "finalize":
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
        finalizer = None
        for each in tasks:
            if each.task_type == "finalize":
                finalizer = each
                continue
            task_id = str(uuid4())
            tasks_array.append(task_id)
            each.task_key = task_id
            each.callback_queue = self.arbiter_id
            if callback:
                each.callback = True
        for each in tasks:
            if each.task_type == "finalize":
                continue
            for task in self.add_task(each):
                self.state["groups"][group_id].append(task)
        if callback:
            callback.tasks_array = tasks_array
            callback.task_key = group_id
            callback.callback_queue = self.arbiter_id
            callback.task_type = "callback"
            if finalizer:
                callback.callback = True
                tasks_array.append(callback.task_key)
            for task in self.add_task(callback):
                self.state["groups"][group_id].append(task)
        if finalizer:
            finalizer.tasks_array = tasks_array
            finalizer.task_key = group_id
            finalizer.callback_queue = self.arbiter_id
            for task in self.add_task(finalizer):
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
