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


class GlobalEventHandler(BaseEventHandler):

    def _connect_to_specific_queue(self, channel):
        exchange_queue = channel.queue_declare(queue="", exclusive=True)
        channel.queue_bind(
            exchange=self.settings.all,
            queue=exchange_queue.method.queue
        )
        channel.basic_consume(
            queue=exchange_queue.method.queue,
            on_message_callback=self.queue_event_callback,
            auto_ack=True
        )

        logging.info("Waiting for global events")
        return channel

    def queue_event_callback(self, channel, method, properties, body):
        """ Process event """
        _ = properties, self, channel, method
        try:
            event = json.loads(body)
            #
            event_type = event.get("type", None)
            logging.info("[GlobalEvent] Type: %s", event_type)
            #
            if event_type in ["stop_task", "purge_task"]:
                task_key = event.get("task_key")
                if task_key in self.state and not self.state[task_key]["process"].ready():
                    logging.info("[GlobalEvent] Terminating task %s", task_key)
                    self.state[task_key]["status"] = "canceled"
            elif event_type == "subscription_notification":
                subscription = event.get("subscription")
                if subscription in self.subscriptions:
                    logging.info("[GlobalEvent] Got data for subscription %s", subscription)
                    self.subscriptions[subscription] = event.get("data")
            elif event_type == "state":
                message = {"queue": self.state["queue"], "active": self.state["active_workers"],
                           "total": self.state["total_workers"],
                           "available": self.state["total_workers"] - self.state["active_workers"],
                           "type": "state"}
                logging.debug(json.dumps(message, indent=2))
                self.respond(channel, message, event["arbiter"])
            elif event_type == "task_state":
                response = {"type": "task_state"}
                for each in event.get("tasks", []):
                    if each in self.state:
                        response[each] = True if self.state[each]["status"] != "done" else False
                self.respond(channel, response, event["arbiter"])
            elif event_type == "clear_state":
                for task in event.get("tasks", []):
                    if task in self.state:
                        self.state.pop(task)
        except:  # pylint: disable=W0702
            logging.exception("[GlobalEvent] Got exception")
