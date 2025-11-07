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
            #
            join_threads_on_stop=False,
            shutdown_in_thread=False,
            shutdown_join_timeout=5.0,
            shutdown_via_destroy=False,
            #
            sockopt_linger=1000,
            #
            sockopt_tcp_keepalive=None,
            sockopt_tcp_keepalive_cnt=None,
            sockopt_tcp_keepalive_idle=None,
            sockopt_tcp_keepalive_intvl=None,
            #
            sockopt_immediate=None,
            #
            sockopt_heartbeat_ivl=None,
            sockopt_heartbeat_ttl=None,
            sockopt_heartbeat_timeout=None,
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
            "shutdown_via_destroy": shutdown_via_destroy,
            "sockopt_linger": sockopt_linger,
            "sockopt_tcp_keepalive": sockopt_tcp_keepalive,
            "sockopt_tcp_keepalive_cnt": sockopt_tcp_keepalive_cnt,
            "sockopt_tcp_keepalive_idle": sockopt_tcp_keepalive_idle,
            "sockopt_tcp_keepalive_intvl": sockopt_tcp_keepalive_intvl,
            "sockopt_immediate": sockopt_immediate,
            "sockopt_heartbeat_ivl": sockopt_heartbeat_ivl,
            "sockopt_heartbeat_ttl": sockopt_heartbeat_ttl,
            "sockopt_heartbeat_timeout": sockopt_heartbeat_timeout,
        }
        #
        self.retry_interval = retry_interval
        self.mute_first_failed_connections = mute_first_failed_connections
        self.failed_connections = 0
        #
        self.zmq_gevent = is_runtime_gevent()
        #
        self.join_threads_on_stop = join_threads_on_stop
        self.shutdown_in_thread = shutdown_in_thread
        self.shutdown_join_timeout = shutdown_join_timeout
        self.shutdown_via_destroy = shutdown_via_destroy or self.zmq_gevent
        #
        self.sockopt_linger = sockopt_linger
        #
        self.sockopt_tcp_keepalive = sockopt_tcp_keepalive
        self.sockopt_tcp_keepalive_cnt = sockopt_tcp_keepalive_cnt
        self.sockopt_tcp_keepalive_idle = sockopt_tcp_keepalive_idle
        self.sockopt_tcp_keepalive_intvl = sockopt_tcp_keepalive_intvl
        #
        self.sockopt_immediate = sockopt_immediate
        #
        self.sockopt_heartbeat_ivl = sockopt_heartbeat_ivl
        self.sockopt_heartbeat_ttl = sockopt_heartbeat_ttl
        self.sockopt_heartbeat_timeout = sockopt_heartbeat_timeout
        #
        self.zeromq_connect_sub = connect_sub
        self.zeromq_connect_push = connect_push
        #
        self.zeromq_topic = topic_format.format(topic).encode("utf-8")
        #
        self.zmq_ctx = None

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
        if self.shutdown_via_destroy:
            self.zmq_ctx.destroy(self.sockopt_linger)
        else:
            self.zmq_ctx.term()
        #
        if self.join_threads_on_stop:
            self.listening_thread.join(timeout=(self.sockopt_linger * 1.5) / 1000.0)
            #
            for emitting_thread in self.emitting_threads:
                emitting_thread.join(timeout=(self.sockopt_linger * 1.5) / 1000.0)

    def _set_sockopts(self, zmq, zmq_socket):
        if self.sockopt_linger is not None:
            zmq_socket.setsockopt(zmq.LINGER, self.sockopt_linger)
        #
        if self.sockopt_tcp_keepalive is not None:
            zmq_socket.setsockopt(zmq.TCP_KEEPALIVE, self.sockopt_tcp_keepalive)
        if self.sockopt_tcp_keepalive_cnt is not None:
            zmq_socket.setsockopt(zmq.TCP_KEEPALIVE_CNT, self.sockopt_tcp_keepalive_cnt)
        if self.sockopt_tcp_keepalive_idle is not None:
            zmq_socket.setsockopt(zmq.TCP_KEEPALIVE_IDLE, self.sockopt_tcp_keepalive_idle)
        if self.sockopt_tcp_keepalive_intvl is not None:
            zmq_socket.setsockopt(zmq.TCP_KEEPALIVE_INTVL, self.sockopt_tcp_keepalive_intvl)
        #
        if self.sockopt_immediate is not None:
            zmq_socket.setsockopt(zmq.IMMEDIATE, self.sockopt_immediate)
        #
        if self.sockopt_heartbeat_ivl is not None:
            zmq_socket.setsockopt(zmq.HEARTBEAT_IVL, self.sockopt_heartbeat_ivl)
        if self.sockopt_heartbeat_ttl is not None:
            zmq_socket.setsockopt(zmq.HEARTBEAT_TTL, self.sockopt_heartbeat_ttl)
        if self.sockopt_heartbeat_timeout is not None:
            zmq_socket.setsockopt(zmq.HEARTBEAT_TIMEOUT, self.sockopt_heartbeat_timeout)

    def emitting_worker(self):
        """ Emitting thread: emit event data from emit_queue """
        if self.zmq_gevent:
            import zmq.green as zmq  # pylint: disable=C0415,E0401
        else:
            import zmq  # pylint: disable=C0415,E0401
        #
        zmq_socket_push = self.zmq_ctx.socket(zmq.PUSH)  # pylint: disable=E1101
        self._set_sockopts(zmq, zmq_socket_push)
        zmq_socket_push.connect(self.zeromq_connect_push)
        #
        time.sleep(1)  # TODO: use ZMQ monitor later (e.g. wait_for_connected)
        self.emitting_ready_event.set()
        #
        while self.running:
            try:
                data = self.emit_queue.get(timeout=self.queue_get_timeout)
                zmq_socket_push.send_multipart([self.zeromq_topic, data])
            except queue.Empty:
                pass
            except:  # pylint: disable=W0702
                if self.running and self.log_errors:
                    log.exception("Error during event emitting, skipping")
        #
        log.debug("ZeroMQ emitting thread stopping")
        #
        zmq_socket_push.close(linger=self.sockopt_linger)
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
        self._set_sockopts(zmq, zmq_socket_sub)
        zmq_socket_sub.connect(self.zeromq_connect_sub)
        zmq_socket_sub.subscribe(self.zeromq_topic)
        #
        time.sleep(1)  # TODO: use ZMQ monitor later (e.g. wait_for_connected)
        self.listening_ready_event.set()
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
        zmq_socket_sub.close(linger=self.sockopt_linger)
        #
        log.debug("ZeroMQ listening thread exiting")
