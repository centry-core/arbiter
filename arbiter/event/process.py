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


class ProcessEventHandler(BaseEventHandler):
    def __init__(self, settings, subscriptions, state, process_id):
        super().__init__(settings, subscriptions, state)
        self.process_id = process_id

    def _connect_to_specific_queue(self, channel):
        channel.queue_declare(
            queue=self.process_id, durable=True
        )
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(
            queue=self.process_id,
            on_message_callback=self.queue_event_callback
        )
        logging.info("[%s] Waiting for task events", self.ident)
        return channel

    def queue_event_callback(self, channel, method, properties, body):
        """ Process event """
        _ = properties, self, channel, method
        try:
            logging.info(f"[ProcessHandler] {body}")
            event = json.loads(body)
            event_type = event.get("type", None)
            logging.info("[ProcessEvent] Type: %s", event_type)
            if event_type == "subscription_notification":
                subscription = event.get("subscription")
                if subscription in self.subscriptions:
                    self.subscriptions[subscription] = event.get("data")
            if event_type == "task_state":
                event.pop("type")
                for key, value in event.items():
                    if not value and self.process_id in self.state and key not in self.state[self.process_id]["done"]:
                        self.state[self.process_id]["done"].append(key)
                    elif value and self.process_id in self.state and key not in self.state[self.process_id]["running"]:
                        self.state[self.process_id]["running"].append(key)
                logging.info(self.state)
        except:  # pylint: disable=W0702
            logging.exception("[ProcessEvent] Got exception")
        channel.basic_ack(delivery_tag=method.delivery_tag)
