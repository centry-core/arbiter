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
            bind_pub="tcp://*:5010",
            bind_pull="tcp://*:5011",
            join_threads_on_stop=False,
            shutdown_in_thread=True,
            shutdown_join_timeout=5.0,
    ):
        self.bind_pub = bind_pub
        self.bind_pull = bind_pull
        #
        self.join_threads_on_stop = join_threads_on_stop
        self.shutdown_in_thread = shutdown_in_thread
        self.shutdown_join_timeout = shutdown_join_timeout
        #
        self.stop_event = threading.Event()
        self.started = False
        #
        self.gevent_runtime = is_runtime_gevent()
        #
        self.zmq_ctx = None
        self.zmq_linger = 5
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
        self.zmq_ctx.term()
        #
        if self.join_threads_on_stop:
            self.zmq_server_thread.join(timeout=self.zmq_linger * 1.5)


class ZeroMQServerThread(threading.Thread):  # pylint: disable=R0903
    """ ZeroMQ: push from pull """

    def __init__(self, node):
        super().__init__(daemon=True)
        #
        self.node = node

    def run(self):
        """ Run thread """
        if self.node.gevent_runtime:
            import zmq.green as zmq  # pylint: disable=C0415,E0401,E1101
        else:
            import zmq  # pylint: disable=C0415,E0401,E1101
        #
        zmq_socket_pub = self.node.zmq_ctx.socket(zmq.PUB)  # pylint: disable=E1101
        zmq_socket_pub.setsockopt(zmq.LINGER, self.node.zmq_linger)
        zmq_socket_pub.bind(self.node.bind_pub)
        #
        zmq_socket_pull = self.node.zmq_ctx.socket(zmq.PULL)  # pylint: disable=E1101
        zmq_socket_pull.setsockopt(zmq.LINGER, self.node.zmq_linger)
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
        zmq_socket_pull.close(linger=self.node.zmq_linger)
        zmq_socket_pub.close(linger=self.node.zmq_linger)
        #
        log.debug("ZeroMQ server thread exiting")
