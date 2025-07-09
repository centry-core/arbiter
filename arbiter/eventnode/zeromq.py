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
    Event node
"""

import time

try:
    import pylon  # pylint: disable=C0415,E0401,W0611
    from tools import context  # pylint: disable=C0415,E0401
    #
    if context.web_runtime == "gevent":
        import zmq.green as zmq  # pylint: disable=C0415,E0401
    else:
        import zmq  # pylint: disable=E0401
except:  # pylint: disable=W0702
    import zmq  # pylint: disable=E0401

from arbiter import log

from .base import EventNodeBase


class ZeroMQEventNode(EventNodeBase):  # pylint: disable=R0902
    """ Event node (ZMQ) - allows to subscribe to events and to emit new events """

    def __init__(
            self, connect_sub, connect_push, topic="events", topic_format="[{}]",
            hmac_key=None, hmac_digest="sha512", callback_workers=1,
            mute_first_failed_connections=0,
            log_errors=True,
            retry_interval=3.0,
    ):  # pylint: disable=R0913,R0914
        super().__init__(hmac_key, hmac_digest, callback_workers, log_errors)
        #
        self.clone_config = {
            "type": "ZeroMQEventNode",
            "connect_sub": connect_sub,
            "connect_push": connect_push,
            "topic": topic,
            "topic_format": topic_format,
            "hmac_key": hmac_key,
            "hmac_digest": hmac_digest,
            "callback_workers": callback_workers,
            "mute_first_failed_connections": mute_first_failed_connections,
            "log_errors": log_errors,
            "retry_interval": retry_interval,
        }
        #
        self.retry_interval = retry_interval
        self.mute_first_failed_connections = mute_first_failed_connections
        self.failed_connections = 0
        #
        self.zeromq_connect_sub = connect_sub
        self.zeromq_connect_push = connect_push
        #
        self.zeromq_topic = topic_format.format(topic).encode("utf-8")
        #
        self.zmq_ctx = None
        self.zmq_socket_sub = None
        self.zmq_socket_push = None

    def start(self, emit_only=False):
        """ Start event node """
        if self.started:
            return
        #
        self.zmq_ctx = zmq.Context()
        #
        self.zmq_socket_push = self.zmq_ctx.socket(zmq.PUSH)
        self.zmq_socket_push.connect(self.zeromq_connect_push)
        #
        super().start(emit_only)

    def stop(self):
        """ Stop event node """
        super().stop()
        #
        if self.started:
            if self.zmq_socket_push is not None:
                self.zmq_socket_push.close(linger=10)
            #
            if self.zmq_socket_sub is not None:
                self.zmq_socket_sub.close(linger=10)
            #
            self.zmq_ctx.term()

    def emit_data(self, data):
        """ Emit event data """
        self.zmq_socket_push.send_multipart([self.zeromq_topic, data])

    def listening_worker(self):
        """ Listening thread: push event data to sync_queue """
        while self.running:
            try:
                self.zmq_socket_sub = self.zmq_ctx.socket(zmq.SUB)
                self.zmq_socket_sub.connect(self.zeromq_connect_sub)
                self.zmq_socket_sub.setsockopt(zmq.SUBSCRIBE, self.zeromq_topic)
                #
                self.ready_event.set()
                #
                while self.running:
                    topic, message = self.zmq_socket_sub.recv_multipart()
                    #
                    if topic != self.zeromq_topic:
                        continue
                    #
                    if not message:
                        continue
                    #
                    self.sync_queue.put(message)
            except:  # pylint: disable=W0702
                if self.running and self.log_errors:
                    log.exception(
                        "Exception in listening thread. Retrying in %s seconds", self.retry_interval
                    )
                #
                try:
                    self.zmq_socket_sub.close(linger=10)
                except:  # pylint: disable=W0702
                    pass
                #
                if self.running:
                    time.sleep(self.retry_interval)
            finally:
                try:
                    self.zmq_socket_sub.close(linger=10)  # TODO: handle ZeroMQ errors
                except:  # pylint: disable=W0702
                    pass
