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

from arbiter import log


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
        try:
            self._add_self()
        except:  # pylint: disable=W0702
            log.exception("Adding self failed")
        #
        # Loop
        #
        while not self.node.stop_event.is_set():
            time.sleep(self.node.work_interval)
            now = time.time()
            #
            # Announce
            #
            if now - self.last_announce >= self.node.announce_interval:
                try:
                    self._announce(now)
                except:  # pylint: disable=W0702
                    log.exception("Announce failed")
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
        self.fire_event("node_leaving", self.node.node_id)

    def _announce(self, now):
        healthy = self.get_health()
        #
        self.last_announce = now
        self.node.event_node.emit(
            "presence_join",
            {
                "node_id": self.node.node_id,
                "node_pool": self.node.node_pool,
                "node_meta": self.node.node_meta,
                #
                "healthy": healthy,
            },
        )
        #
        events = []
        #
        with self.node.lock:
            node_data = self.node.nodes[self.node.node_id]
            node_state = self.node.node_state[self.node.node_id]
            # Props
            node_data["node_pool"] = self.node.node_pool
            node_data["healthy"] = healthy
            # Announce time
            node_state["last_announce"] = now
            # Health
            if healthy != node_state["healthy"]:
                event = "node_healthy" if healthy else "node_unhealthy"
                events.append(
                    (event, self.node.node_id)
                )
                node_state["healthy"] = healthy
            # Pool
            prev_pool = node_state["node_pool"]
            if self.node.node_pool != prev_pool:
                # Move
                if self.node.node_id in self.node.pools[prev_pool]:
                    self.node.pools[prev_pool].remove(self.node.node_id)
                #
                if self.node.node_pool not in self.node.pools:
                    self.node.pools[self.node.node_pool] = []
                #
                if self.node.node_id not in self.node.pools[self.node.node_pool]:
                    self.node.pools[self.node.node_pool].append(self.node.node_id)
                # Check
                self._check_pools([prev_pool, self.node.node_pool], events, locked=True)
        # Events
        for event, target in events:
            self.fire_event(event, target)

    def _check_pools(self, pools, events, locked=False):
        for pool in pools:
            pool_health = self.get_pool_health(pool, locked=locked)
            #
            if pool not in self.node.pool_state:
                events.append(
                    ("pool_added", pool)
                )
                #
                self.node.pool_state[pool] = {
                    "healthy": pool_health,
                }
                #
                event = "pool_healthy" if pool_health else "pool_unhealthy"
                events.append(
                    (event, pool)
                )
            #
            if pool_health != self.node.pool_state[pool]["healthy"]:
                event = "pool_healthy" if pool_health else "pool_unhealthy"
                events.append(
                    (event, pool)
                )
                self.node.pool_state[pool]["healthy"] = pool_health
            #
            if not self.node.pools[pool]:
                events.append(
                    ("pool_removed", pool)
                )
                #
                self.node.pool_state.pop(pool, None)
                self.node.pools.pop(pool, None)

    def _add_self(self):
        self_healthy = self.get_health()
        self_node_added = False
        self_pool_added = False
        #
        with self.node.lock:
            # Node data
            if self.node.node_id not in self.node.nodes:
                self.node.nodes[self.node.node_id] = {
                    "node_id": self.node.node_id,
                    "node_pool": self.node.node_pool,
                    "node_meta": self.node.node_meta,
                    #
                    "healthy": self_healthy,
                }
            # Node state
            if self.node.node_id not in self.node.node_state:
                self.node.node_state[self.node.node_id] = {
                    "state": "present",
                    "node_pool": self.node.node_pool,
                    "healthy": self_healthy,
                    "last_announce": time.time(),
                    "missing_from": None,
                }
                #
                self_node_added = True
            # Pool data
            if self.node.node_pool not in self.node.pools:
                self.node.pools[self.node.node_pool] = []
            #
            if self.node.node_id not in self.node.pools[self.node.node_pool]:
                self.node.pools[self.node.node_pool].append(self.node.node_id)
            # Pool state
            if self.node.node_pool not in self.node.pool_state:
                self.node.pool_state[self.node.node_pool] = {
                    "healthy": self_healthy,
                }
                #
                self_pool_added = True
        #
        if self_node_added:
            self.fire_event("node_joined", self.node.node_id)
            self.fire_event(
                "node_healthy" if self_healthy else "node_unhealthy",
                self.node.node_id,
            )
        #
        if self_pool_added:
            self.fire_event("pool_added", self.node.node_pool)
            self.fire_event(
                "pool_healthy" if self_healthy else "pool_unhealthy",
                self.node.node_pool,
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
        #
        node_pool = payload.get("node_pool")
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
        #
        with self.node.lock:
            self.node.nodes.pop(node_id, None)
            for pool in self.node.pools.values():
                if node_id in pool:
                    pool.remove(node_id)
                    # TODO: pool removed

    #
    # Tools
    #

    def fire_event(self, event, target):
        """ Tool """
        with self.node.lock:
            callbacks = list(self.node.callbacks.get(event, []))
        #
        for callback in callbacks:
            try:
                callback(self.node, event, target)
            except:  # pylint: disable=W0702
                log.exception("Callback failed")

    def get_pool_health(self, pool, locked=False):
        """ Tool """
        if pool not in self.node.pools or not self.node.pools[pool]:
            return False
        #
        if not locked:
            with self.node.lock:
                nodes = list(self.node.pools[pool])
        else:
            nodes = list(self.node.pools[pool])
        #
        healthy = True
        #
        for node_id in nodes:
            if not self.node.node_state.get(node_id, {}).get("healthy", False):
                healthy = False
                break
        #
        return healthy

    def get_health(self, locked=False):
        """ Tool """
        healthy = True
        #
        if not locked:
            with self.node.lock:
                callbacks = list(self.node.health_checks)
        else:
            callbacks = list(self.node.health_checks)
        #
        for callback in callbacks:
            try:
                if not callback(self.node):
                    healthy = False
                    break
            except:  # pylint: disable=W0702
                log.exception("Health check failed")
                #
                healthy = False
                break
        #
        return healthy
