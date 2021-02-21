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
    Event node

    Allows to emit and consume global events

    Emit an event: main thread
    Listen for events from queue: thread one
    Run callbacks: thread two

    Listening and running threads are synced using local Queue

    Event payloads are serialized, gziped and signed before sending to queue
"""

import threading
import pickle
import queue
import time
import gzip
import hmac

import pika  # pylint: disable=E0401

from arbiter import log
from arbiter.config import Config


class EventNode:  # pylint: disable=R0902
    """ Event node - allows to subscribe to events and to emit new events """

    def __init__(
            self, host, port, user, password, vhost="carrier", event_queue="events",
            hmac_key=None, hmac_digest="sha512", callback_workers=1,
            ssl_context=None, ssl_server_hostname=None,
    ):  # pylint: disable=R0913
        self.queue_config = Config(host, port, user, password, vhost, event_queue, all_queue=None)
        self.event_callbacks = dict()  # event_name -> [callbacks]
        #
        self.ssl_context = ssl_context
        self.ssl_server_hostname = ssl_server_hostname
        #
        self.hmac_key = hmac_key
        self.hmac_digest = hmac_digest
        if self.hmac_key is not None and isinstance(self.hmac_key, str):
            self.hmac_key = self.hmac_key.encode("utf-8")
        #
        self.retry_interval = 3.0
        #
        self.stop_event = threading.Event()
        self.event_lock = threading.Lock()
        self.sync_queue = queue.Queue()
        #
        self.listening_thread = threading.Thread(target=self._listening_worker, daemon=True)
        self.callback_threads = list()
        for _ in range(callback_workers):
            self.callback_threads.append(
                threading.Thread(target=self._callback_worker, daemon=True)
            )
        #
        self.ready_event = threading.Event()
        self.started = False

    def start(self):
        """ Start event node """
        if self.started:
            return
        #
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
            if event_name not in self.event_callbacks:
                self.event_callbacks[event_name] = list()
            if callback not in self.event_callbacks[event_name]:
                self.event_callbacks[event_name].append(callback)

    def unsubscribe(self, event_name, callback):
        """ Unsubscribe from event """
        with self.event_lock:
            if event_name not in self.event_callbacks:
                return
            if callback not in self.event_callbacks[event_name]:
                return
            self.event_callbacks[event_name].remove(callback)

    def emit(self, event_name, payload=None):
        """ Emit event with payload data """
        connection = self._get_connection()
        channel = self._get_channel(connection)
        #
        event = {
            "name": event_name,
            "payload": payload,
        }
        body = gzip.compress(pickle.dumps(event, protocol=pickle.HIGHEST_PROTOCOL))
        if self.hmac_key is not None:
            digest = hmac.digest(self.hmac_key, body, self.hmac_digest)
            body = body + digest
        #
        channel.basic_publish(
            exchange=self.queue_config.queue,
            routing_key="",
            body=body,
            properties=pika.BasicProperties(
                delivery_mode=2
            )
        )
        #
        connection.close()

    def _listening_worker(self):
        while self.running:
            try:
                connection = self._get_connection()
                channel = self._get_channel(connection)
                #
                exchange_queue = channel.queue_declare(queue="", exclusive=True)
                channel.queue_bind(
                    exchange=self.queue_config.queue,
                    queue=exchange_queue.method.queue
                )
                channel.basic_consume(
                    queue=exchange_queue.method.queue,
                    on_message_callback=self._listening_callback,
                    auto_ack=True
                )
                #
                self.ready_event.set()
                #
                channel.start_consuming()
            except:  # pylint: disable=W0702
                log.exception(
                    "Exception in listening thread. Retrying in %s seconds", self.retry_interval
                )
                time.sleep(self.retry_interval)
            finally:
                try:
                    connection.close()
                except:  # pylint: disable=W0702
                    pass

    def _listening_callback(self, channel, method, properties, body):
        _ = channel, method, properties
        self.sync_queue.put(body)

    def _callback_worker(self):
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
                        log.error("Invalid event digest, skipping")
                        continue
                #
                event = pickle.loads(gzip.decompress(body))
                #
                event_name = event.get("name")
                event_payload = event.get("payload")
                #
                with self.event_lock:
                    if event_name not in self.event_callbacks:
                        continue
                    callbacks = self.event_callbacks[event_name].copy()
                #
                for callback in callbacks:
                    try:
                        callback(event_name, event_payload)
                    except:  # pylint: disable=W0702
                        log.exception("Event callback failed, skipping")
            except:  # pylint: disable=W0702
                log.exception("Error during event processing, skipping")

    def _get_connection(self):
        while self.running:
            try:
                #
                pika_ssl_options = None
                if self.ssl_context is not None:
                    pika_ssl_options = pika.SSLOptions(self.ssl_context, self.ssl_server_hostname)
                #
                connection = pika.BlockingConnection(
                    pika.ConnectionParameters(
                        host=self.queue_config.host,
                        port=self.queue_config.port,
                        virtual_host=self.queue_config.vhost,
                        credentials=pika.PlainCredentials(
                            self.queue_config.user,
                            self.queue_config.password
                        ),
                        ssl_options=pika_ssl_options,
                    )
                )
                connection.process_data_events()
                return connection
            except:  # pylint: disable=W0702
                log.exception(
                    "Failed to create connection. Retrying in %s seconds", self.retry_interval
                )
                time.sleep(self.retry_interval)

    def _get_channel(self, connection):
        channel = connection.channel()
        channel.exchange_declare(
            exchange=self.queue_config.queue,
            exchange_type="fanout",
            durable=True
        )
        return channel


class MockEventNode:  # pylint: disable=R0902
    """ Event node - allows to subscribe to events and to emit new events - local-only mock """

    def __init__(self):  # pylint: disable=R0913
        self.event_callbacks = dict()  # event_name -> [callbacks]
        self.started = True

    def start(self):
        """ Start event node """

    def stop(self):
        """ Stop event node """

    @property
    def running(self):
        """ Check if it is time to stop """
        return True

    def subscribe(self, event_name, callback):
        """ Subscribe to event """
        if event_name not in self.event_callbacks:
            self.event_callbacks[event_name] = list()
        if callback not in self.event_callbacks[event_name]:
            self.event_callbacks[event_name].append(callback)

    def unsubscribe(self, event_name, callback):
        """ Unsubscribe from event """
        if event_name not in self.event_callbacks:
            return
        if callback not in self.event_callbacks[event_name]:
            return
        self.event_callbacks[event_name].remove(callback)

    def emit(self, event_name, payload=None):
        """ Emit event with payload data """
        if event_name not in self.event_callbacks:
            return
        for callback in self.event_callbacks[event_name]:
            try:
                callback(event_name, payload)
            except:  # pylint: disable=W0702
                log.exception("Event callback failed, skipping")
