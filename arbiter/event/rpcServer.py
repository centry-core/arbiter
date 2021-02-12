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


import pika
import json
import logging
from traceback import format_exc
from arbiter.event.base import BaseEventHandler


class RPCEventHandler(BaseEventHandler):
    def __init__(self, settings, subscriptions, state, task_registry, wait_time=2.0):
        super().__init__(settings, subscriptions, state, wait_time=wait_time)
        self.task_registry = task_registry

    def _connect_to_specific_queue(self, channel):
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(
            queue=self.settings.queue,
            on_message_callback=self.queue_event_callback
        )
        logging.info("[%s] Waiting for task events", self.ident)
        return channel

    @staticmethod
    def rpc_respond(channel, queue, body, correlation_id):
        channel.basic_publish(exchange='', routing_key=queue,
                              body=json.dumps(body).encode("utf-8"),
                              properties=pika.BasicProperties(
                                  correlation_id=correlation_id
                              ))

    def queue_event_callback(self, channel, method, properties, body):  # pylint: disable=R0912,R0915
        event = json.loads(body)
        try:
            logging.info("[%s] [RPCEvent]", self.ident)
            logging.info("[%s] [RPCEvent] Starting worker process", self.ident)
            if event.get("task_name") not in self.task_registry:
                raise ModuleNotFoundError("Task is not a part of this worker")
            result = self.task_registry[event.get("task_name")](*event.get("args", []), **event.get("kwargs", {}))
            logging.info("[%s] [TaskEvent] Worker process stopped", self.ident)
            self.rpc_respond(channel, properties.reply_to,
                             {"type": "result", "message": result, "task_key": event.get("task_key")},
                             properties.correlation_id)
        except:
            logging.exception("[%s] [TaskEvent] Got exception", self.ident)
            self.rpc_respond(channel, properties.reply_to,
                             {"type": "exception", "message": format_exc(), "task_key": event.get("task_key")},
                             properties.correlation_id)
        channel.basic_ack(delivery_tag=method.delivery_tag)
