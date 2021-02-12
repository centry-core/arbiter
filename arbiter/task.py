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
