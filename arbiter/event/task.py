import json
import logging
import multiprocessing
mp = multiprocessing.get_context("spawn")
from uuid import uuid4
from traceback import format_exc
from arbiter.event.base import BaseEventHandler

from arbiter.task import ProcessWatcher


class TaskEventHandler(BaseEventHandler):
    def __init__(self, settings, subscriptions, state, task_registry, wait_time=2.0, pool_size=1):
        super().__init__(settings, subscriptions, state, wait_time=wait_time)
        self.pool_size = pool_size
        self.task_registry = task_registry
        self.pool = mp.Pool(pool_size)

    def _connect_to_specific_queue(self, channel):
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(
            queue=self.settings.queue,
            on_message_callback=self.queue_event_callback
        )
        logging.info("[%s] Waiting for task events", self.ident)
        return channel

    def queue_event_callback(self, channel, method, properties, body):  # pylint: disable=R0912,R0915
        _ = properties, self, channel, method
        event = json.loads(body)
        try:
            self.state["active_workers"] += 1
            # new tasks will be w/o key testing purpose only
            # do not use in prod implementation
            if not event.get("task_key"):
                event["task_key"] = uuid4()
            event_type = event.get("type", "task")
            logging.info("[%s] [TaskEvent] Type: %s", self.ident, event_type)
            if event_type == "task":
                if event.get("arbiter"):
                    self.respond(channel, {"type": "task_state_change", "task_key": event.get("task_key"),
                                           "task_state": "running"}, event.get("arbiter"))
                logging.info("[%s] [TaskEvent] Starting worker process", self.ident)
                if event.get("task_name") not in self.task_registry:
                    raise ModuleNotFoundError("Task is not a part of this worker")
                worker = self.pool.apply_async(self.task_registry[event.get("task_name")],
                                               event.get("args", []),
                                               event.get("kwargs", {}))
                self.state[event.get("task_key")] = {
                    "process": worker,
                    "status": "running"
                }
                while not worker.ready():
                    if self.state[event.get('task_key')]["status"] == "canceled":
                        self.pool.close()
                        self.pool.terminate()
                        self.pool.join()
                        self.pool = mp.Pool(self.pool_size)
                        break
                    channel._connection.sleep(1.0)  # pylint: disable=W0212

                result = worker.get() if self.state[event.get('task_key')]["status"] != "canceled" else "canceled"
                logging.info("[%s] [TaskEvent] Worker process stopped", self.ident)
                if event.get("arbiter"):
                    self.respond(channel, {"type": "task_state_change", "task_key": event.get("task_key"),
                                           "result": result, "task_state": "done"}, event.get("arbiter"))
                self.state[event.get('task_key')]["status"] = "done"
                if not event.get("callback", False):
                    self.state.pop(event.get("task_key"))

            elif event_type == "callback":
                callback_key = event.get("task_key")
                minibitter = ProcessWatcher(callback_key, self.settings.host, self.settings.port, self.settings.user,
                                            self.settings.password, wait_time=self.wait_time)
                state = minibitter.collect_state(event.get("tasks_array"))
                if all(task in state.get("done", []) for task in event.get("tasks_array")):
                    event["type"] = "task"
                    minibitter.clear_state(event.get("tasks_array"))
                    minibitter.close()
                    event.pop("tasks_array")
                    self.respond(channel, event, self.settings.queue)
                else:
                    minibitter.close()
                    self.respond(channel, event, self.settings.queue, 60)
        except:
            logging.exception("[%s] [TaskEvent] Got exception", self.ident)
            if event.get("arbiter"):
                self.respond(channel, {"type": "task_state_change", "task_key": event.get("task_key"),
                                       "result": format_exc(), "task_state": "exception"}, event.get("arbiter"))
        self.state["active_workers"] -= 1
        channel.basic_ack(delivery_tag=method.delivery_tag)
