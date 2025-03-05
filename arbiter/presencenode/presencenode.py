#!/usr/bin/python3
# coding=utf-8
# pylint: disable=C0116,C0302

#   Copyright 2023-2025 getcarrier.io
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

"""
    Presence node

    Allows to track nodes

    Uses existing EventNode as a transport
"""

import uuid
import threading

from .worker import PresenceNodeWorker


class PresenceNode:  # pylint: disable=R0902,R0904
    """ Presence node - track nodes """

    def __init__(  # pylint: disable=R0913,R0914
            self, event_node,
            node_id=None,
            node_pool=None,
            node_meta=None,
            id_prefix="",
            work_interval=1,  # seconds
            announce_interval=5,  # seconds
            max_missing_announces=3,  # intervals after which node is declared missing
            auto_leaving_intervals=60,  # intervals in missing state after which node is lost
    ):
        self.event_node = event_node
        self.event_node_was_started = False
        #
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self.started = False
        #
        self.id_prefix = id_prefix
        self.node_pool = node_pool
        #
        self.node_id = node_id if node_id is not None else self.generate_node_id()
        self.node_meta = node_meta.copy() if node_meta is not None else {}
        #
        self.work_interval = work_interval
        self.announce_interval = announce_interval
        self.max_missing_announces = max_missing_announces
        self.auto_leaving_intervals = auto_leaving_intervals
        #
        self.pools = {}  # node_pool -> [node_id]
        self.nodes = {}  # node_id -> {id, pool, meta}
        #
        self.pool_state = {}
        self.node_state = {}
        #
        self.health_checks = []
        self.callbacks = {}
        #
        self.worker = PresenceNodeWorker(self)

    #
    # Node start and stop
    #

    def start(self, block=False):
        """ Start node """
        if self.started:
            return
        #
        self.stop_event.clear()
        #
        if not self.event_node.started:
            self.event_node.start()
            self.event_node_was_started = True
        #
        self.event_node.subscribe("presence_join", self.worker.on_presence_join)
        self.event_node.subscribe("presence_leave", self.worker.on_presence_leave)
        #
        self.worker.start()
        self.started = True
        #
        if block:
            self.stop_event.wait()

    def stop(self):
        """ Stop task node """
        self.event_node.unsubscribe("presence_leave", self.worker.on_presence_leave)
        self.event_node.unsubscribe("presence_join", self.worker.on_presence_join)
        #
        if self.event_node_was_started:
            self.event_node.stop()
        #
        self.started = False
        self.stop_event.set()
        #
        self.worker.join(timeout=3.0)

    #
    # Healthcheck registration
    #

    def register_healthcheck(self, callback):
        """ Register healthcheck """
        with self.lock:
            if callback not in self.health_checks:
                self.health_checks.append(callback)

    def unregister_healthcheck(self, callback):
        """ Unregister healthcheck """
        with self.lock:
            if callback in self.health_checks:
                self.health_checks.remove(callback)

    #
    # Callback registration
    #

    def register_callback(self, event, callback):
        """ Register callback """
        with self.lock:
            if event not in self.callbacks:
                self.callbacks[event] = []
            #
            if callback not in self.callbacks[event]:
                self.callbacks[event].append(callback)

    def unregister_callback(self, event, callback):
        """ Unregister callback """
        with self.lock:
            if event not in self.callbacks:
                return
            #
            if callback in self.callbacks[event]:
                self.callbacks[event].remove(callback)

    #
    # Helpers
    #

    def get_node(self, node_id):
        """ Helper """
        return None

    def is_node_present(self, node_id):
        """ Helper """
        return False

    def is_node_healthy(self, node_id):
        """ Helper """
        return False

    def get_pool(self, node_pool):
        """ Helper """
        return None

    def is_pool_present(self, node_pool):
        return False

    def is_pool_healthy(self, node_pool):
        return False

    #
    # Tools
    #

    def generate_node_id(self):
        """ Get 'mostly' safe new node_id """
        node_id = f"{self.id_prefix}{str(uuid.uuid4())}"
        #
        return node_id
