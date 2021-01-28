import pika
from time import sleep
from uuid import uuid4
from json import loads, dumps
import logging

from arbiter.event.base import BaseEventHandler


class RPCClintEventHandler(BaseEventHandler):
    def __init__(self, settings, subscriptions, state):
        super().__init__(settings, subscriptions, state)
        self.callback_queue = None
        self.correlation_id = None
        self.response = None
        self.client = self._get_channel()

    def _connect_to_specific_queue(self, channel):
        result = channel.queue_declare(queue='', exclusive=True)
        self.callback_queue = result.method.queue
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.queue_event_callback,
            auto_ack=True
        )
        logging.info("[%s] Waiting for task events", self.ident)
        return channel

    def queue_event_callback(self, channel, method, properties, body):  # pylint: disable=R0912,R0915
        if self.correlation_id == properties.correlation_id:
            self.response = body

    def call(self, tasks_module, task, args, kwargs):
        self.response = None
        self.correlation_id = str(uuid4())
        message = {
            "task_name": task,
            "args": args,
            "kwargs": kwargs
        }
        logging.info(message)
        try:
            self.client.basic_publish(
                exchange='',
                routing_key=tasks_module,
                properties=pika.BasicProperties(
                    reply_to=self.callback_queue,
                    correlation_id=self.correlation_id,
                ),
                body=dumps(message).encode("utf-8"))
        except (pika.exceptions.ConnectionClosedByBroker,
                pika.exceptions.AMQPChannelError,
                pika.exceptions.AMQPConnectionError,
                pika.exceptions.StreamLostError):
            sleep(0.1)
            self.client = self._get_channel()
            return self.call(tasks_module, task, args, kwargs)
        while self.response is None:
            self.client.connection.process_data_events()
        resp = loads(self.response)
        if resp.get("type") == "exception":
            raise ChildProcessError(resp["message"])
        return resp.get("message")

