#!/usr/bin/python3
# coding=utf-8

#   Copyright 2021 getcarrier.io
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
    RPC node

    Allows to call and register RPC functions

    Uses existing EventNode as a transport
"""

import uuid
import queue
import threading
import functools

from arbiter import log


class RpcNode:  # pylint: disable=R0902
    """ RPC node - register and call remote functions """

    def __init__(self, event_node, id_prefix=None):
        self.event_node = event_node
        self.event_node_was_started = False
        #
        self.functions = dict()
        self.requests = dict()
        #
        self.id_prefix = id_prefix if id_prefix is not None else ""
        #
        self.lock = threading.Lock()
        self.proxy = RpcProxy(self)
        #
        self.started = False

    def start(self):
        """ Start RPC node """
        if self.started:
            return
        #
        if not self.event_node.started:
            self.event_node.start()
            self.event_node_was_started = True
        #
        self.event_node.subscribe("rpc_request", self._rpc_request_callback)
        self.event_node.subscribe("rpc_response", self._rpc_response_callback)
        #
        self.started = True

    def stop(self):
        """ Stop RPC node """
        self.event_node.unsubscribe("rpc_request", self._rpc_request_callback)
        self.event_node.unsubscribe("rpc_response", self._rpc_response_callback)
        #
        if self.event_node_was_started:
            self.event_node.stop()

    def register(self, func, name=None):
        """ Register RPC function """
        if name is None:
            name = self._get_callable_name(func)
        #
        with self.lock:
            self.functions[name] = func

    def unregister(self, func, name=None):
        """ Unregister RPC function """
        if name is None:
            name = self._get_callable_name(func)
        #
        with self.lock:
            if name in self.functions:
                self.functions.pop(name)

    def call(self, func, *args, **kvargs):
        """ Invoke RPC function """
        if not self.started:
            raise RuntimeError("RpcNode is not started")
        #
        if func in self.functions:
            return self.functions[func](*args, **kvargs)
        #
        request_id = self._make_request(func, args, kvargs)
        response = self.requests[request_id].get()
        with self.lock:
            self.requests.pop(request_id)
        #
        if "raise" in response:
            raise response.get("raise")
        return response.get("return", None)

    def call_with_timeout(self, func, timeout, *args, **kvargs):
        """ Invoke RPC function with timeout """
        if not self.started:
            raise RuntimeError("RpcNode is not started")
        #
        if func in self.functions:
            return self.functions[func](*args, **kvargs)
        #
        request_id = self._make_request(func, args, kvargs)
        try:
            response = self.requests[request_id].get(timeout=timeout)
        finally:
            with self.lock:
                self.requests.pop(request_id)
        #
        if "raise" in response:
            raise response.get("raise")
        return response.get("return", None)

    def timeout(self, timeout):
        """ Get RpcTimeoutProxy instance """
        return RpcTimeoutProxy(self, timeout)

    def _rpc_request_callback(self, event_name, event_payload):
        _ = event_name
        #
        for key in ["request_id", "func", "args", "kvargs"]:
            if key not in event_payload:
                log.error("Invalid RPC request, skipping")
                return
        #
        if event_payload["func"] not in self.functions:
            return
        #
        try:
            return_data = self.functions[event_payload["func"]](
                *event_payload["args"], **event_payload["kvargs"]
            )
            self.event_node.emit(
                "rpc_response",
                {
                    "request_id": event_payload["request_id"],
                    "return": return_data,
                }
            )
        except BaseException as exception_data:  # pylint: disable=W0703
            log.exception("RPC function exception")
            self.event_node.emit(
                "rpc_response",
                {
                    "request_id": event_payload["request_id"],
                    "raise": exception_data,
                }
            )

    def _rpc_response_callback(self, event_name, event_payload):
        _ = event_name
        #
        for key in ["request_id"]:
            if key not in event_payload:
                log.error("Invalid RPC response, skipping")
                return
        #
        if event_payload["request_id"] not in self.requests:
            return
        #
        self.requests[event_payload["request_id"]].put(event_payload)

    def _make_request(self, func, args, kvargs):
        # Generate request ID
        with self.lock:
            while True:
                request_id = f"{self.id_prefix}{str(uuid.uuid4())}"
                if request_id not in self.requests:
                    self.requests[request_id] = queue.Queue()
                    break
        # Emit request event
        self.event_node.emit(
            "rpc_request",
            {
                "request_id": request_id,
                "func": func,
                "args": args,
                "kvargs": kvargs,
            }
        )
        # Return request ID
        return request_id

    def _get_callable_name(self, func):
        if hasattr(func, "__name__"):
            return func.__name__
        if isinstance(func, functools.partial):
            return self._get_callable_name(func.func)
        raise ValueError("Cannot guess callable name")


class RpcProxy:  # pylint: disable=R0903
    """ RPC proxy - syntax sugar for RPC calls """
    def __init__(self, rpc_node):
        self.__rpc_node = rpc_node

    def __invoke(self, func, *args, **kvargs):
        return self.__rpc_node.call(func, *args, **kvargs)

    def __getattr__(self, name):
        return functools.partial(self.__invoke, name)


class RpcTimeoutProxy:  # pylint: disable=R0903
    """ RPC proxy - syntax sugar for RPC calls - with timeout support """
    def __init__(self, rpc_node, timeout):
        self.__rpc_node = rpc_node
        self.__timeout = timeout

    def __invoke(self, func, *args, **kvargs):
        return self.__rpc_node.call_with_timeout(func, self.__timeout, *args, **kvargs)

    def __getattr__(self, name):
        return functools.partial(self.__invoke, name)
