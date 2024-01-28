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


class EventNodeBase:  # pylint: disable=R0902
    """ Event node (base) - allows to subscribe to events and to emit new events """

    def __init__(
            self,
            hmac_key=None, hmac_digest="sha512",
            callback_workers=1,
            log_errors=True,
    ):  # pylint: disable=R0913
        self.log_errors = log_errors
        self.event_callbacks = {}  # event_name -> [callbacks]
        self.catch_all_callbacks = []
        #
        self.hmac_key = hmac_key
        self.hmac_digest = hmac_digest
        if self.hmac_key is not None and isinstance(self.hmac_key, str):
            self.hmac_key = self.hmac_key.encode("utf-8")
        #
        self.stop_event = threading.Event()
        self.event_lock = threading.Lock()
        self.sync_queue = queue.Queue()
        #
        self.listening_thread = threading.Thread(target=self.listening_worker, daemon=True)
        self.callback_threads = []
        for _ in range(callback_workers):
            self.callback_threads.append(
                threading.Thread(target=self.callback_worker, daemon=True)
            )
        #
        self.ready_event = threading.Event()
        self.started = False

    def start(self, emit_only=False):
        """ Start event node """
        if self.started:
            return
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
        event = {
            "name": event_name,
            "payload": payload,
        }
        body = gzip.compress(pickle.dumps(
            event, protocol=pickle.HIGHEST_PROTOCOL
        ))
        if self.hmac_key is not None:
            digest = hmac.digest(self.hmac_key, body, self.hmac_digest)
            body = body + digest
        #
        self.emit_data(body)

    def emit_data(self, data):
        """ Emit event data """
        raise NotImplementedError

    def listening_worker(self):
        """ Listening thread: push event data to sync_queue """
        raise NotImplementedError

    def callback_worker(self):
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
                    try:
                        callback(event_name, event_payload)
                    except:  # pylint: disable=W0702
                        if self.log_errors:
                            log.exception("Event callback failed, skipping")
            except:  # pylint: disable=W0702
                if self.log_errors:
                    log.exception("Error during event processing, skipping")
