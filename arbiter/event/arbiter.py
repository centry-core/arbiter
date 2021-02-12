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


import json
import logging

from arbiter.event.base import BaseEventHandler


class ArbiterEventHandler(BaseEventHandler):
    def __init__(self, settings, subscriptions, state, arbiter_id):
        super().__init__(settings, subscriptions, state)
        self.arbiter_id = arbiter_id

    def _connect_to_specific_queue(self, channel):
        channel.queue_declare(
            queue=self.arbiter_id, durable=True
        )
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(
            queue=self.arbiter_id,
            on_message_callback=self.queue_event_callback
        )
        logging.info("[%s] Waiting for task events", self.ident)
        return channel

    def queue_event_callback(self, channel, method, properties, body):  # pylint: disable=R0912,R0915
        _ = properties, self, channel, method
        event = json.loads(body)
        try:
            event_type = event.get("type")
            logging.info("[%s] [ArbiterEvent] Type: %s", self.ident, event_type)
            if event.get("task_key") and event.get("task_key") not in self.state:
                self.state[event.get("task_key")] = {}
            if event_type in ["task_state_change"]:
                self.state[event.get("task_key")]["state"] = event.get("task_state")
                if event.get("result"):
                    self.state[event.get("task_key")]["result"] = event.get("result")
            if event_type == "state":
                queue = event["queue"]
                del event["type"]
                del event["queue"]
                if "state" not in self.state:
                    self.state["state"] = {}
                if queue not in self.state["state"]:
                    self.state["state"][queue] = {}
                for key, value in event.items():
                    if key not in self.state["state"][queue]:
                        self.state["state"][queue][key] = 0
                    self.state["state"][queue][key] += value
            if event_type == "result":
                self.state[event.get("task_key")]["state"] = "done"
                self.state[event.get("task_key")]["result"] = event.get("message")
        except:
            logging.exception("[%s] [TaskEvent] Got exception", self.ident)
        channel.basic_ack(delivery_tag=method.delivery_tag)
