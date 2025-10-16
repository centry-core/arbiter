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
    Server node
"""

import threading

from arbiter import log
from ..tools.pylon import is_runtime_gevent


class ZeroMQServerNode:  # pylint: disable=R0902,R0904
    """ Server node - ZeroMQ """

    def __init__(  # pylint: disable=R0913,R0914
            self,
            #
            bind_pub="tcp://*:5010",
            bind_pull="tcp://*:5011",
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
    ):
        self.gevent_runtime = is_runtime_gevent()
        #
        self.bind_pub = bind_pub
        self.bind_pull = bind_pull
        #
        self.join_threads_on_stop = join_threads_on_stop
        self.shutdown_in_thread = shutdown_in_thread
        self.shutdown_join_timeout = shutdown_join_timeout
        self.shutdown_via_destroy = shutdown_via_destroy or self.gevent_runtime
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
        self.stop_event = threading.Event()
        self.started = False
        #
        self.zmq_ctx = None
        self.zmq_server_thread = None

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
        if self.gevent_runtime:
            import zmq.green as zmq  # pylint: disable=C0415,E0401,E1101
        else:
            import zmq  # pylint: disable=C0415,E0401,E1101
        #
        self.zmq_ctx = zmq.Context()
        #
        self.zmq_server_thread = ZeroMQServerThread(self)
        self.zmq_server_thread.start()
        #
        self.started = True
        #
        if block:
            self.stop_event.wait()

    def stop(self):
        """ Stop task node """
        if not self.started:
            return
        #
        self.started = False
        self.stop_event.set()
        #
        log.debug("Stop initiated")
        #
        if self.shutdown_in_thread:
            shutdown_thread = threading.Thread(target=self.shutdown, daemon=True)
            shutdown_thread.start()
            shutdown_thread.join(timeout=self.shutdown_join_timeout)
        else:
            self.shutdown()

    def shutdown(self):
        """ Perform stop actions """
        if self.shutdown_via_destroy:
            self.zmq_ctx.destroy(self.sockopt_linger)
        else:
            self.zmq_ctx.term()
        #
        if self.join_threads_on_stop:
            self.zmq_server_thread.join(timeout=(self.sockopt_linger * 1.5) / 1000.0)


class ZeroMQServerThread(threading.Thread):  # pylint: disable=R0903
    """ ZeroMQ: push from pull """

    def __init__(self, node):
        super().__init__(daemon=True)
        #
        self.node = node

    def _set_sockopts(self, zmq, zmq_socket):
        if self.node.sockopt_linger is not None:
            zmq_socket.setsockopt(zmq.LINGER, self.node.sockopt_linger)
        #
        if self.node.sockopt_tcp_keepalive is not None:
            zmq_socket.setsockopt(zmq.TCP_KEEPALIVE, self.node.sockopt_tcp_keepalive)
        if self.node.sockopt_tcp_keepalive_cnt is not None:
            zmq_socket.setsockopt(zmq.TCP_KEEPALIVE_CNT, self.node.sockopt_tcp_keepalive_cnt)
        if self.node.sockopt_tcp_keepalive_idle is not None:
            zmq_socket.setsockopt(zmq.TCP_KEEPALIVE_IDLE, self.node.sockopt_tcp_keepalive_idle)
        if self.node.sockopt_tcp_keepalive_intvl is not None:
            zmq_socket.setsockopt(zmq.TCP_KEEPALIVE_INTVL, self.node.sockopt_tcp_keepalive_intvl)
        #
        if self.node.sockopt_immediate is not None:
            zmq_socket.setsockopt(zmq.IMMEDIATE, self.node.sockopt_immediate)
        #
        if self.node.sockopt_heartbeat_ivl is not None:
            zmq_socket.setsockopt(zmq.HEARTBEAT_IVL, self.node.sockopt_heartbeat_ivl)
        if self.node.sockopt_heartbeat_ttl is not None:
            zmq_socket.setsockopt(zmq.HEARTBEAT_TTL, self.node.sockopt_heartbeat_ttl)
        if self.node.sockopt_heartbeat_timeout is not None:
            zmq_socket.setsockopt(zmq.HEARTBEAT_TIMEOUT, self.node.sockopt_heartbeat_timeout)

    def run(self):
        """ Run thread """
        if self.node.gevent_runtime:
            import zmq.green as zmq  # pylint: disable=C0415,E0401,E1101
        else:
            import zmq  # pylint: disable=C0415,E0401,E1101
        #
        # Pub
        #
        zmq_socket_pub = self.node.zmq_ctx.socket(zmq.PUB)  # pylint: disable=E1101
        self._set_sockopts(zmq, zmq_socket_pub)
        zmq_socket_pub.bind(self.node.bind_pub)
        #
        # Pull
        #
        zmq_socket_pull = self.node.zmq_ctx.socket(zmq.PULL)  # pylint: disable=E1101
        self._set_sockopts(zmq, zmq_socket_pull)
        zmq_socket_pull.bind(self.node.bind_pull)
        #
        while not self.node.stop_event.is_set():
            try:
                frame = zmq_socket_pull.recv_multipart()
                zmq_socket_pub.send_multipart(frame)
            except:  # pylint: disable=W0702
                if not self.node.stop_event.is_set():
                    log.exception("Exception in ZeroMQ server thread, continuing")
        #
        log.debug("ZeroMQ server thread stopping")
        #
        zmq_socket_pull.close(linger=self.node.sockopt_linger)
        zmq_socket_pub.close(linger=self.node.sockopt_linger)
        #
        log.debug("ZeroMQ server thread exiting")
