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

import hmac
import gzip
import queue
import pickle
import threading

from arbiter import log

from .tools import make_event_node
from . import hooks


class EventNodeBase:  # pylint: disable=R0902
    """ Event node (base) - allows to subscribe to events and to emit new events """

    def __init__(
            self,
            hmac_key=None, hmac_digest="sha512",
            callback_workers=1,
            log_errors=True,
    ):  # pylint: disable=R0913
        self.clone_config = None
        #
        self.log_errors = log_errors
        self.event_callbacks = {}  # event_name -> [callbacks]
        self.catch_all_callbacks = []
        #
        self.before_callback_hooks = []
        self.after_callback_hooks = []
        #
        self.hmac_key = hmac_key
        self.hmac_digest = hmac_digest
        if self.hmac_key is not None and isinstance(self.hmac_key, str):
            self.hmac_key = self.hmac_key.encode("utf-8")
        #
        self.stop_event = threading.Event()
        self.event_lock = threading.Lock()
        self.sync_queue = queue.Queue()
        self.emit_queue = queue.Queue()
        #
        self.emitting_thread = threading.Thread(target=self.emitting_worker, daemon=True)
        self.listening_thread = threading.Thread(target=self.listening_worker, daemon=True)
        self.callback_threads = []
        for _ in range(callback_workers):
            self.callback_threads.append(
                threading.Thread(target=self.callback_worker, daemon=True)
            )
        #
        self.ready_event = threading.Event()
        self.can_emit = True
        self.started = False

    def clone(self):
        """ Make new event node with same config """
        if self.clone_config is None:
            raise NotImplementedError
        #
        return make_event_node(config=self.clone_config)

    def start(self, emit_only=False):
        """ Start event node """
        if self.started:
            return
        #
        self.emitting_thread.start()
        #
        if emit_only:
            self.ready_event.set()
        else:
            self.listening_thread.start()
            for callback_thread in self.callback_threads:
                callback_thread.start()
        #
        self.ready_event.wait()
        self.started = True

    def stop(self):
        """ Stop event node """
        self.stop_event.set()

    @property
    def running(self):
        """ Check if it is time to stop """
        return not self.stop_event.is_set()

    def subscribe(self, event_name, callback):
        """ Subscribe to event """
        with self.event_lock:
            if event_name is ...:
                if callback not in self.catch_all_callbacks:
                    self.catch_all_callbacks.append(callback)
                return
            #
            if event_name not in self.event_callbacks:
                self.event_callbacks[event_name] = []
            if callback not in self.event_callbacks[event_name]:
                self.event_callbacks[event_name].append(callback)

    def unsubscribe(self, event_name, callback):
        """ Unsubscribe from event """
        with self.event_lock:
            if event_name is ...:
                if callback in self.catch_all_callbacks:
                    self.catch_all_callbacks.remove(callback)
                return
            #
            if event_name not in self.event_callbacks:
                return
            if callback not in self.event_callbacks[event_name]:
                return
            self.event_callbacks[event_name].remove(callback)

    def emit(self, event_name, payload=None):
        """ Emit event with payload data """
        if not self.can_emit:
            return
        #
        data = self.make_event_data(event_name, payload)
        self.emit_queue.put(data)

    def make_event_data(self, event_name, payload=None):
        """ Make event data """
        event = {
            "name": event_name,
            "payload": payload,
        }
        #
        data = gzip.compress(pickle.dumps(
            event, protocol=pickle.HIGHEST_PROTOCOL
        ))
        #
        if self.hmac_key is not None:
            digest = hmac.digest(self.hmac_key, data, self.hmac_digest)
            data = data + digest
        #
        return data

    def emit_data(self, data):
        """ Emit event data """
        raise NotImplementedError

    def listening_worker(self):
        """ Listening thread: push event data to sync_queue """
        raise NotImplementedError

    def add_before_callback_hook(self, hook):
        """ Register pre-callback hook """
        with self.event_lock:
            if hook not in self.before_callback_hooks:
                self.before_callback_hooks.append(hook)

    def remove_before_callback_hook(self, hook):
        """ De-register pre-callback hook """
        with self.event_lock:
            while hook in self.before_callback_hooks:
                self.before_callback_hooks.remove(hook)

    def add_after_callback_hook(self, hook):
        """ Register post-callback hook """
        with self.event_lock:
            if hook not in self.after_callback_hooks:
                self.after_callback_hooks.append(hook)

    def remove_after_callback_hook(self, hook):
        """ De-register post-callback hook """
        with self.event_lock:
            while hook in self.after_callback_hooks:
                self.after_callback_hooks.remove(hook)

    def emitting_worker(self):  # pylint: disable=R0912
        """ Emitting thread: emit data """
        while self.running:
            try:
                data = self.emit_queue.get()
                self.emit_data(data)
            except:  # pylint: disable=W0702
                if self.log_errors:
                    log.exception("Error during event emitting, skipping")

    def callback_worker(self):  # pylint: disable=R0912
        """ Callback thread: call subscribers """
        while self.running:
            try:
                body = self.sync_queue.get()
                #
                if self.hmac_key is not None:
                    hmac_obj = hmac.new(self.hmac_key, digestmod=self.hmac_digest)
                    hmac_size = hmac_obj.digest_size
                    #
                    body_digest = body[-hmac_size:]
                    body = body[:-hmac_size]
                    #
                    digest = hmac.digest(self.hmac_key, body, self.hmac_digest)
                    #
                    if not hmac.compare_digest(body_digest, digest):
                        if self.log_errors:
                            log.error("Invalid event digest, skipping")
                        continue
                #
                event = pickle.loads(gzip.decompress(body))
                #
                event_name = event.get("name")
                event_payload = event.get("payload")
                #
                with self.event_lock:
                    callbacks = self.catch_all_callbacks.copy()
                    if event_name in self.event_callbacks:
                        callbacks.extend(self.event_callbacks[event_name])
                #
                for callback in callbacks:
                    for hook in hooks.before_callback_hooks + self.before_callback_hooks:
                        try:
                            hook(callback, event_name, event_payload)
                        except:  # pylint: disable=W0702
                            if self.log_errors:
                                log.exception("Before callback hook failed, skipping")
                    #
                    try:
                        callback_result = callback(event_name, event_payload)
                    except:  # pylint: disable=W0702
                        if self.log_errors:
                            log.exception("Event callback failed, skipping")
                        #
                        callback_result = None  # FIXME: pass exceptions to after_callback_hooks?
                    #
                    for hook in hooks.after_callback_hooks + self.after_callback_hooks:
                        try:
                            hook(callback, callback_result, event_name, event_payload)
                        except:  # pylint: disable=W0702
                            if self.log_errors:
                                log.exception("After callback hook failed, skipping")
            except:  # pylint: disable=W0702
                if self.log_errors:
                    log.exception("Error during event processing, skipping")
