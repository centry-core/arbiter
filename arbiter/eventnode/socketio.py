#!/usr/bin/python3
# coding=utf-8

#   Copyright 2024 getcarrier.io
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
    Event node
"""

import time

import socketio  # pylint: disable=E0401

from arbiter import log

from .base import EventNodeBase


class SocketIOEventNode(EventNodeBase):  # pylint: disable=R0902
    """ Event node (Socket.IO) - allows to subscribe to events and to emit new events """

    def __init__(
            self, url, password, room="events",
            hmac_key=None, hmac_digest="sha512", callback_workers=1,
            mute_first_failed_connections=0,
            ssl_verify=False, socketio_path="socket.io",
            log_errors=True,
    ):  # pylint: disable=R0913
        super().__init__(hmac_key, hmac_digest, callback_workers, log_errors)
        #
        self.sio_config = {
            "url": url,
            "password": password,
            "room": room,
            "ssl_verify": ssl_verify,
            "socketio_path": socketio_path,
        }
        #
        self.retry_interval = 3.0
        #
        self.mute_first_failed_connections = mute_first_failed_connections
        self.failed_connections = 0
        #
        self.sio = None

    def start(self, emit_only=False):
        """ Start event node """
        if self.started:
            return
        #
        self.sio = self._get_connection()
        #
        super().start(emit_only)

    def stop(self):
        """ Stop event node """
        super().stop()
        #
        if self.started:
            self.sio.disconnect()

    def emit_data(self, data):
        """ Emit event data """
        with self.event_lock:
            self.sio.emit("eventnode_event", data)

    def listening_worker(self):
        """ Listening thread: push event data to sync_queue """
        while self.running:
            try:
                self.sio.on("eventnode_event", self._listening_callback)
                self.ready_event.set()
                self.sio.wait()
            except:  # pylint: disable=W0702
                if self.log_errors:
                    log.exception(
                        "Exception in listening thread. Retrying in %s seconds", self.retry_interval
                    )
                time.sleep(self.retry_interval)
            finally:
                try:
                    pass  # TODO: handle socketio errors
                except:  # pylint: disable=W0702
                    pass

    def _listening_callback(self, body):
        self.sync_queue.put(body)

    def _get_connection(self):
        while self.running:
            try:
                sio = socketio.Client(
                    ssl_verify=self.sio_config["ssl_verify"],
                )
                #
                sio.connect(
                    url=self.sio_config["url"],
                    socketio_path=self.sio_config["socketio_path"],
                )
                #
                with self.event_lock:
                    sio.emit("eventnode_join", {
                        "password": self.sio_config["password"],
                        "room": self.sio_config["room"],
                    })
                #
                return sio
            except:  # pylint: disable=W0702
                if self.log_errors and \
                        self.failed_connections >= self.mute_first_failed_connections:
                    log.exception(
                        "Failed to create connection. Retrying in %s seconds", self.retry_interval
                    )
                #
                self.failed_connections += 1
                time.sleep(self.retry_interval)
        #
        return None
