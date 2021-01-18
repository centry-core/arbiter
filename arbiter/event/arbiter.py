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
        logging.info("[%s] [TaskEvent] Got event", self.ident)
        event = json.loads(body)
        try:
            event_type = event.get("type")
            logging.info("[%s] [TaskEvent] Type: %s", self.ident, event_type)
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
        except:
            logging.exception("[%s] [TaskEvent] Got exception", self.ident)
        channel.basic_ack(delivery_tag=method.delivery_tag)
