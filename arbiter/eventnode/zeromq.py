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
import queue
import threading

from arbiter import log

from .base import EventNodeBase
from ..tools.pylon import is_runtime_gevent


class ZeroMQEventNode(EventNodeBase):  # pylint: disable=R0902
    """ Event node (ZMQ) - allows to subscribe to events and to emit new events """

    def __init__(
            self, connect_sub, connect_push, topic="events", topic_format="[{}]",
            hmac_key=None, hmac_digest="sha512", callback_workers=1,
            mute_first_failed_connections=0,
            log_errors=True,
            retry_interval=3.0,
            join_threads_on_stop=False,
            shutdown_in_thread=True,
            shutdown_join_timeout=5.0,
    ):  # pylint: disable=R0913,R0914
        super().__init__(
            hmac_key, hmac_digest, callback_workers, log_errors,
            use_emit_queue=True,
        )
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
            "join_threads_on_stop": join_threads_on_stop,
            "shutdown_in_thread": shutdown_in_thread,
            "shutdown_join_timeout": shutdown_join_timeout,
        }
        #
        self.retry_interval = retry_interval
        self.mute_first_failed_connections = mute_first_failed_connections
        self.failed_connections = 0
        #
        self.join_threads_on_stop = join_threads_on_stop
        self.shutdown_in_thread = shutdown_in_thread
        self.shutdown_join_timeout = shutdown_join_timeout
        #
        self.zeromq_connect_sub = connect_sub
        self.zeromq_connect_push = connect_push
        #
        self.zeromq_topic = topic_format.format(topic).encode("utf-8")
        #
        self.zmq_gevent = is_runtime_gevent()
        #
        self.zmq_ctx = None
        self.zmq_linger = 5

    def start(self, emit_only=False):
        """ Start event node """
        if self.started:
            return
        #
        if self.zmq_gevent:
            import zmq.green as zmq  # pylint: disable=C0415,E0401
        else:
            import zmq  # pylint: disable=C0415,E0401
        #
        self.zmq_ctx = zmq.Context()
        #
        super().start(emit_only)

    def stop(self):
        """ Stop event node """
        super().stop()
        #
        if self.started:
            log.debug("Stop initiated")
            #
            if self.shutdown_in_thread:
                shutdown_thread = threading.Thread(target=self.shutdown, daemon=True)
                shutdown_thread.start()
                shutdown_thread.join(timeout=self.shutdown_join_timeout)
            else:
                self.shutdown()
            #
            # FIXME: should set started to false?

    def shutdown(self):
        """ Perform stop actions """
        self.zmq_ctx.term()
        #
        if self.join_threads_on_stop:
            self.listening_thread.join(timeout=self.zmq_linger * 1.5)
            self.emitting_thread.join(timeout=self.zmq_linger * 1.5)

    def emitting_worker(self):
        """ Emitting thread: emit event data from emit_queue """
        if self.zmq_gevent:
            import zmq.green as zmq  # pylint: disable=C0415,E0401
        else:
            import zmq  # pylint: disable=C0415,E0401
        #
        zmq_socket_push = self.zmq_ctx.socket(zmq.PUSH)  # pylint: disable=E1101
        zmq_socket_push.setsockopt(zmq.LINGER, self.zmq_linger)
        zmq_socket_push.connect(self.zeromq_connect_push)
        #
        while self.running:
            try:
                data = self.emit_queue.get(self.queue_get_timeout)
                zmq_socket_push.send_multipart([self.zeromq_topic, data])
            except queue.Empty:
                pass
            except:  # pylint: disable=W0702
                if self.running and self.log_errors:
                    log.exception("Error during event emitting, skipping")
        #
        log.debug("ZeroMQ emitting thread stopping")
        #
        zmq_socket_push.close(linger=self.zmq_linger)
        #
        log.debug("ZeroMQ emitting thread exiting")


    def listening_worker(self):  # pylint: disable=R0912
        """ Listening thread: push event data to sync_queue """
        if self.zmq_gevent:
            import zmq.green as zmq  # pylint: disable=C0415,E0401
        else:
            import zmq  # pylint: disable=C0415,E0401
        #
        zmq_socket_sub = self.zmq_ctx.socket(zmq.SUB)  # pylint: disable=E1101
        zmq_socket_sub.setsockopt(zmq.LINGER, self.zmq_linger)
        zmq_socket_sub.connect(self.zeromq_connect_sub)
        zmq_socket_sub.subscribe(self.zeromq_topic)
        #
        time.sleep(1)  # NOTE: use ZMQ monitor later (e.g. wait_for_connected)
        #
        self.ready_event.set()
        #
        while self.running:
            try:
                topic, message = zmq_socket_sub.recv_multipart()  # pylint: disable=W0632
                #
                if topic != self.zeromq_topic:
                    continue
                #
                if not message:
                    continue
                #
                self.sync_queue.put(message)
            except:  # pylint: disable=W0702
                if self.running:
                    if self.log_errors:
                        log.exception(
                            "Exception in listening thread. Retrying in %s seconds",
                            self.retry_interval,
                        )
                    #
                    time.sleep(self.retry_interval)
        #
        log.debug("ZeroMQ listening thread stopping")
        #
        zmq_socket_sub.close(linger=self.zmq_linger)
        #
        log.debug("ZeroMQ listening thread exiting")
