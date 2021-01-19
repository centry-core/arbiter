import time
import json
import threading
import pika
import logging


class BaseEventHandler(threading.Thread):
    """ Basic representation of events handler"""

    def __init__(self, settings, subscriptions, state, wait_time=2.0):
        super().__init__(daemon=True)
        self.settings = settings
        self.state = state
        self.subscriptions = subscriptions
        self._stop_event = threading.Event()
        self.started = False
        self.wait_time = wait_time

    def _get_channel(self):
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=self.settings.host,
                port=self.settings.port,
                virtual_host=self.settings.vhost,
                credentials=pika.PlainCredentials(
                    self.settings.user,
                    self.settings.password
                )
            )
        )
        channel = connection.channel()
        if self.settings.queue:
            channel.queue_declare(
                queue=self.settings.queue, durable=True
            )
        channel.exchange_declare(
            exchange=self.settings.all,
            exchange_type="fanout", durable=True
        )
        channel = self._connect_to_specific_queue(channel)
        return channel

    def _connect_to_specific_queue(self, channel):
        raise NotImplemented

    def wait_running(self):
        while not self.started:
            time.sleep(0.5)

    def run(self):
        """ Run handler thread """
        logging.info("Starting handler thread")
        while not self.stopped():
            logging.info("Starting handler consuming")
            try:
                channel = self._get_channel()
                logging.info("[%s] Waiting for task events", self.ident)
                self.started = True
                channel.start_consuming()
            except pika.exceptions.ConnectionClosedByBroker:
                logging.info("Connection Closed by Broker")
                time.sleep(5.0)
                continue
            except pika.exceptions.AMQPChannelError:
                logging.info("AMQPChannelError")
            except pika.exceptions.StreamLostError:
                logging.info("Recovering from error")
                time.sleep(5.0)
                continue
            except pika.exceptions.AMQPConnectionError:
                logging.info("Recovering from error")
                time.sleep(5.0)
                continue
        channel.stop_consuming()

    def stop(self):
        self._stop_event.set()

    def stopped(self):
        return self._stop_event.is_set()

    @staticmethod
    def respond(channel, message, queue, delay=0):
        logging.debug(message)
        headers = {}
        if delay and isinstance(delay, int):
            headers = {"x-delay": delay}
        channel.basic_publish(
            exchange="", routing_key=queue,
            body=json.dumps(message).encode("utf-8"),
            properties=pika.BasicProperties(
                delivery_mode=2,
                headers=headers,
            )
        )

    def queue_event_callback(self, channel, method, properties, body):  # pylint: disable=R0912,R0915
        raise NotImplemented
