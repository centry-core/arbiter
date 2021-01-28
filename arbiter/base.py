import logging
import pika

from uuid import uuid4
from json import dumps

from arbiter.config import Config
from arbiter.event.arbiter import ArbiterEventHandler

connection = None
channel = None


class Base:
    def __init__(self, host, port, user, password, vhost="carrier", queue=None, all_queue="arbiterAll", wait_time=2.0):
        self.config = Config(host, port, user, password, vhost, queue, all_queue)
        self.state = dict()
        self.wait_time = wait_time

    def _get_connection(self):
        global connection
        global channel
        if not connection:
            connection = pika.BlockingConnection(
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
        if not channel:
            channel = connection.channel()
            if self.config.queue:
                channel.queue_declare(
                    queue=self.config.queue, durable=True
                )
            channel.exchange_declare(
                exchange=self.config.all,
                exchange_type="fanout", durable=True
            )
        try:
            connection.process_data_events()
        except:
            connection = None
            channel = None
            return self._get_connection()
        return channel

    @staticmethod
    def disconnect():
        global connection
        global channel
        if connection:
            connection.close()
        connection = None
        channel = None

    def send_message(self, msg, reply_to="", queue="", exchange=""):
        self._get_connection().basic_publish(
            exchange=exchange, routing_key=queue,
            body=dumps(msg).encode("utf-8"),
            properties=pika.BasicProperties(
                reply_to=reply_to,
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
            task_key = str(uuid4()) if task.task_key == "" else task.task_key
            tasks.append(task_key)
            if task.callback_queue and task_key not in self.state:
                self.state[task_key] = {
                    "task_type": task.task_type,
                    "state": "initiated"
                }
            logging.debug(f"Task body {task.to_json()}")
            message = task.to_json()
            message["task_key"] = task_key
            self.send_message(message, reply_to=task.callback_queue, queue=task.queue)
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
            self.disconnect()
