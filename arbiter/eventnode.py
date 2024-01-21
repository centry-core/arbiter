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
import importlib
import pickle
import queue
import time
import gzip
import hmac
import ssl
import os

import pika  # pylint: disable=E0401
# import pika_pool  # pylint: disable=E0401
from redis import StrictRedis  # pylint: disable=E0401
import socketio  # pylint: disable=E0401

from arbiter import log
from arbiter.config import Config


def make_event_node(config=None, env_prefix="EVENTNODE_"):
    """ Make *EventNode instance """
    if config is None:
        config = {}
        #
        for key, value in os.environ.items():
            if key.startswith(env_prefix):
                config[key[len(env_prefix):].lower()] = value
    #
    eventnode_cfg = config.copy()
    eventnode_type = eventnode_cfg.pop("type")
    #
    eventnode_cls = getattr(
        importlib.import_module("arbiter.eventnode"),
        eventnode_type
    )
    #
    return eventnode_cls(**eventnode_cfg)


class EventNode:  # pylint: disable=R0902
    """ Event node - allows to subscribe to events and to emit new events """

    def __init__(
            self, host, port, user, password, vhost="carrier", event_queue="events",
            hmac_key=None, hmac_digest="sha512", callback_workers=1,
            ssl_context=None, ssl_server_hostname=None,
            mute_first_failed_connections=0,
            use_ssl=False, ssl_verify=False,
    ):  # pylint: disable=R0913
        self.queue_config = Config(host, port, user, password, vhost, event_queue, all_queue=None)
        self.event_callbacks = {}  # event_name -> [callbacks]
        #
        self.ssl_context = ssl_context
        self.ssl_server_hostname = ssl_server_hostname
        #
        if self.ssl_context is None and use_ssl:
            self.ssl_context = ssl.create_default_context()
            if ssl_verify:
                self.ssl_context.verify_mode = ssl.CERT_REQUIRED
                self.ssl_context.check_hostname = True
                self.ssl_context.load_default_certs()
            else:
                self.ssl_context.check_hostname = False
                self.ssl_context.verify_mode = ssl.CERT_NONE
            self.ssl_server_hostname = host
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
        self.callback_threads = []
        for _ in range(callback_workers):
            self.callback_threads.append(
                threading.Thread(target=self._callback_worker, daemon=True)
            )
        #
        self.ready_event = threading.Event()
        self.started = False
        #
        self.mute_first_failed_connections = mute_first_failed_connections
        self.failed_connections = 0
        #
        # self.emit_pool = pika_pool.NullPool(
        #     create=self._get_connection
        # )

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
                self.event_callbacks[event_name] = []
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
        # with self.emit_pool.acquire() as connection:
        #     channel = connection.channel
        #     self._prepare_channel(channel)
        connection = self._get_connection()
        channel = self._get_channel(connection)
        #
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
                if self.failed_connections >= self.mute_first_failed_connections:
                    log.exception(
                        "Failed to create connection. Retrying in %s seconds", self.retry_interval
                    )
                #
                self.failed_connections += 1
                time.sleep(self.retry_interval)
        #
        return None

    def _get_channel(self, connection):
        channel = connection.channel()
        self._prepare_channel(channel)
        return channel

    def _prepare_channel(self, channel):
        channel.exchange_declare(
            exchange=self.queue_config.queue,
            exchange_type="fanout",
            durable=True
        )


class RedisEventNode:  # pylint: disable=R0902
    """ Event node (redis) - allows to subscribe to events and to emit new events """

    def __init__(
            self, host, port, password, event_queue="events",
            hmac_key=None, hmac_digest="sha512", callback_workers=1,
            mute_first_failed_connections=0,
            use_ssl=False,
    ):  # pylint: disable=R0913
        self.redis_config = {
            "host": host,
            "port": port,
            "password": password,
            "event_queue": event_queue,
        }
        self.event_callbacks = {}  # event_name -> [callbacks]
        #
        self.use_ssl = use_ssl
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
        self.callback_threads = []
        for _ in range(callback_workers):
            self.callback_threads.append(
                threading.Thread(target=self._callback_worker, daemon=True)
            )
        #
        self.ready_event = threading.Event()
        self.started = False
        #
        self.mute_first_failed_connections = mute_first_failed_connections
        self.failed_connections = 0
        #
        self.redis = None

    def start(self):
        """ Start event node """
        if self.started:
            return
        #
        self.redis = self._get_connection()
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
                self.event_callbacks[event_name] = []
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
        self.redis.publish(self.redis_config.get("event_queue"), body)

    def _listening_worker(self):
        while self.running:
            try:
                pubsub = self.redis.pubsub(ignore_subscribe_messages=True)
                pubsub.subscribe(self.redis_config.get("event_queue"))
                #
                self.ready_event.set()
                #
                for message in pubsub.listen():
                    if not message:
                        continue
                    #
                    self.sync_queue.put(message.get("data", b""))
            except:  # pylint: disable=W0702
                log.exception(
                    "Exception in listening thread. Retrying in %s seconds", self.retry_interval
                )
                time.sleep(self.retry_interval)
            finally:
                try:
                    pass  # TODO: handle redis errors
                except:  # pylint: disable=W0702
                    pass

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
                redis = StrictRedis(
                    host=self.redis_config.get("host"),
                    port=self.redis_config.get("port", 6379),
                    password=self.redis_config.get("password", None),
                    ssl=self.use_ssl,
                )
                #
                return redis
            except:  # pylint: disable=W0702
                if self.failed_connections >= self.mute_first_failed_connections:
                    log.exception(
                        "Failed to create connection. Retrying in %s seconds", self.retry_interval
                    )
                #
                self.failed_connections += 1
                time.sleep(self.retry_interval)
        #
        return None


class SocketIOEventNode:  # pylint: disable=R0902
    """ Event node (Socket.IO) - allows to subscribe to events and to emit new events """

    def __init__(
            self, url, password, room="events",
            hmac_key=None, hmac_digest="sha512", callback_workers=1,
            mute_first_failed_connections=0,
            ssl_verify=False, socketio_path="socket.io",
    ):  # pylint: disable=R0913
        self.sio_config = {
            "url": url,
            "password": password,
            "room": room,
            "ssl_verify": ssl_verify,
            "socketio_path": socketio_path,
        }
        self.event_callbacks = {}  # event_name -> [callbacks]
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
        self.callback_threads = []
        for _ in range(callback_workers):
            self.callback_threads.append(
                threading.Thread(target=self._callback_worker, daemon=True)
            )
        #
        self.ready_event = threading.Event()
        self.started = False
        #
        self.mute_first_failed_connections = mute_first_failed_connections
        self.failed_connections = 0
        #
        self.sio = None

    def start(self):
        """ Start event node """
        if self.started:
            return
        #
        self.sio = self._get_connection()
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
        #
        if self.started:
            self.sio.disconnect()

    @property
    def running(self):
        """ Check if it is time to stop """
        return not self.stop_event.is_set()

    def subscribe(self, event_name, callback):
        """ Subscribe to event """
        with self.event_lock:
            if event_name not in self.event_callbacks:
                self.event_callbacks[event_name] = []
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
        with self.event_lock:
            self.sio.emit("eventnode_event", body)

    def _listening_worker(self):
        while self.running:
            try:
                self.sio.on("eventnode_event", self._listening_callback)
                self.ready_event.set()
                self.sio.wait()
            except:  # pylint: disable=W0702
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
                if self.failed_connections >= self.mute_first_failed_connections:
                    log.exception(
                        "Failed to create connection. Retrying in %s seconds", self.retry_interval
                    )
                #
                self.failed_connections += 1
                time.sleep(self.retry_interval)
        #
        return None


class MockEventNode:  # pylint: disable=R0902
    """ Event node - allows to subscribe to events and to emit new events - local-only mock """

    def __init__(self):  # pylint: disable=R0913
        self.event_callbacks = {}  # event_name -> [callbacks]
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
            self.event_callbacks[event_name] = []
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
