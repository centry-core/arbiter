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

import time

from redis import StrictRedis  # pylint: disable=E0401

from arbiter import log

from .base import EventNodeBase


class RedisEventNode(EventNodeBase):  # pylint: disable=R0902
    """ Event node (redis) - allows to subscribe to events and to emit new events """

    def __init__(
            self, host, port, password, event_queue="events",
            hmac_key=None, hmac_digest="sha512", callback_workers=1,
            mute_first_failed_connections=0,
            use_ssl=False, ssl_verify=False,
            log_errors=True,
    ):  # pylint: disable=R0913
        super().__init__(hmac_key, hmac_digest, callback_workers, log_errors)
        #
        self.clone_config = {
            "type": "RedisEventNode",
            "host": host,
            "port": port,
            "password": password,
            "event_queue": event_queue,
            "hmac_key": hmac_key,
            "hmac_digest": hmac_digest,
            "callback_workers": callback_workers,
            "mute_first_failed_connections": mute_first_failed_connections,
            "use_ssl": use_ssl,
            "ssl_verify": ssl_verify,
            "log_errors": log_errors,
        }
        #
        self.redis_config = {
            "host": host,
            "port": port,
            "password": password,
            "event_queue": event_queue,
        }
        #
        self.use_ssl = use_ssl
        self.ssl_verify = ssl_verify
        #
        self.retry_interval = 3.0
        #
        self.mute_first_failed_connections = mute_first_failed_connections
        self.failed_connections = 0
        #
        self.redis = None

    def start(self, emit_only=False):
        """ Start event node """
        if self.started:
            return
        #
        self.redis = self._get_connection()
        #
        super().start(emit_only)

    def stop(self):
        """ Stop event node """
        super().stop()
        #
        if self.started:
            self.redis.close()

    def emit_data(self, data):
        """ Emit event data """
        self.redis.publish(self.redis_config.get("event_queue"), data)

    def listening_worker(self):
        """ Listening thread: push event data to sync_queue """
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
                if self.log_errors:
                    log.exception(
                        "Exception in listening thread. Retrying in %s seconds", self.retry_interval
                    )
                time.sleep(self.retry_interval)
            finally:
                try:
                    pass  # TODO: handle redis errors
                except:  # pylint: disable=W0702
                    pass

    def _get_connection(self):
        while self.running:
            try:
                redis = StrictRedis(
                    host=self.redis_config.get("host"),
                    port=self.redis_config.get("port", 6379),
                    password=self.redis_config.get("password", None),
                    ssl=self.use_ssl,
                    ssl_cert_reqs="required" if self.ssl_verify else "none",
                )
                #
                return redis
            except:  # pylint: disable=W0702
                if self.log_errors and \
                        self.failed_connections >= self.mute_first_failed_connections:
                    log.exception(
                        "Failed to create connection. Retrying in %s seconds", self.retry_interval
                    )
                #
                self.failed_connections += 1
                time.sleep(self.retry_interval)
        #
        return None
