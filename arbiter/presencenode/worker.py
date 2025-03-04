#!/usr/bin/python3
# coding=utf-8

#   Copyright 2025 getcarrier.io
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
"""

import time
import threading


class PresenceNodeWorker(threading.Thread):  # pylint: disable=R0903
    """ Perform state updates """

    def __init__(self, node):
        super().__init__(daemon=True)
        #
        self.node = node
        self.last_announce = 0

    def run(self):
        """ Run housekeeper thread """
        #
        # Add self
        #
        with self.node.lock:
            if self.node.node_pool not in self.node.pools:
                self.node.pools[self.node.node_pool] = []
            #
            if self.node.node_id not in self.node.pools[self.node.node_pool]:
                self.node.pools[self.node.node_pool].append(self.node.node_id)
            #
            if self.node.node_id not in self.node.nodes:
                self.node.nodes[self.node.node_id] = {
                    "node_id": self.node.node_id,
                    "node_pool": self.node.node_pool,
                    "node_meta": self.node.node_meta,
                    #
                    "status": "present",
                    "healthy": True,
                    "last_announce": time.time(),
                }
        #
        while not self.node.stop_event.is_set():
            time.sleep(self.node.work_interval)
            now = time.time()
            #
            # Announce
            #
            if now - self.last_announce >= self.node.announce_interval:
                self.last_announce = now
                self.node.event_node.emit(
                    "presence_join",
                    {
                        "node_id": self.node.node_id,
                        "node_pool": self.node.node_pool,
                        "node_meta": self.node.node_meta,
                    },
                )
                self.node.nodes[self.node.node_id]["last_announce"] = now
            #
            # State update
            #
        #
        # Leave
        #
        self.node.event_node.emit(
            "presence_leave",
            {
                "node_id": self.node.node_id,
            },
        )

    #
    # Event handlers
    #

    def on_presence_join(self, event_name, payload):
        """ Process presence event """
        _ = event_name
        #
        node_id = payload.get("node_id")
        if node_id == self.node.node_id:
            return
        # #
        # self.event_node.emit(
        #     "service_provider",
        #     {
        #         "target": payload.get("reply_to"),
        #         "ident": self.ident,
        #     }
        # )

    def on_presence_leave(self, event_name, payload):
        """ Process presence event """
        _ = event_name
        #
        node_id = payload.get("node_id")
        if node_id == self.node.node_id:
            return
