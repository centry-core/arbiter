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
                    if not value and key not in self.state[self.process_id]["done"]:
                        self.state[self.process_id]["done"].append(key)
                    elif value and key not in self.state[self.process_id]["running"]:
                        self.state[self.process_id]["running"].append(key)
                logging.info(self.state)
        except:  # pylint: disable=W0702
            logging.exception("[ProcessEvent] Got exception")
        channel.basic_ack(delivery_tag=method.delivery_tag)
