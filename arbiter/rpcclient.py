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

from arbiter.base import Base

from arbiter.event.rpcClient import RPCClintEventHandler


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
