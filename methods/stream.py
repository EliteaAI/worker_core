#!/usr/bin/python3
# coding=utf-8

#   Copyright 2024 EPAM Systems
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

""" Method """

import uuid
import queue
import traceback

from pylon.core.tools import log  # pylint: disable=E0611,E0401,W0611
from pylon.core.tools import web  # pylint: disable=E0611,E0401,W0611


class Method:  # pylint: disable=E1101,R0903,W0201
    """
        Method Resource

        self is pointing to current Module instance

        web.method decorator takes zero or one argument: method name
        Note: web.method decorator must be the last decorator (at top)
    """

    @web.init()
    def init_streams(self):
        """ Create streams registry """
        self.streams = {}

    @web.method()
    def add_stream(self):
        """ Create stream ID """
        while True:
            stream_id = str(uuid.uuid4())
            if stream_id not in self.streams:
                break
        #
        self.streams[stream_id] = queue.Queue()
        #
        return stream_id

    @web.method()
    def remove_stream(self, stream_id):
        """ Forget stream by ID """
        self.streams.pop(stream_id, None)

    @web.method()
    def on_stream_event(self, _, payload):
        """ Process stream event """
        event = payload.copy()
        #
        stream_id = event.pop("stream_id", None)
        #
        if stream_id not in self.streams:
            return
        #
        self.streams[stream_id].put(event)

    @web.method()
    def stream_chunk(self, stream_id, chunk):
        """ Stream to worker client(s) """
        self.event_node.emit(
            "stream_event",
            {
                "stream_id": stream_id,
                "type": "stream_chunk",
                "data": chunk,
            },
        )

    @web.method()
    def stream_end(self, stream_id):
        """ Stream to worker client(s) """
        self.event_node.emit(
            "stream_event",
            {
                "stream_id": stream_id,
                "type": "stream_end",
                "data": None,
            },
        )

    @web.method()
    def stream_exception(self, stream_id, exception_info=None):
        """ Stream to worker client(s) """
        if exception_info is None:
            exception_info = traceback.format_exc()
        #
        self.event_node.emit(
            "stream_event",
            {
                "stream_id": stream_id,
                "type": "stream_exception",
                "data": exception_info,
            },
        )
