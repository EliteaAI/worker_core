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

import gc
import time
import importlib

from pylon.core.tools import log  # pylint: disable=E0611,E0401,W0611
from pylon.core.tools import web  # pylint: disable=E0611,E0401,W0611

import arbiter


class Method:  # pylint: disable=E1101,R0903,W0201
    """
        Method Resource

        self is pointing to current Module instance

        web.method decorator takes zero or one argument: method name
        Note: web.method decorator must be the last decorator (at top)
    """

    @web.method()
    def preload_model(  # pylint: disable=R0913
            self,
            event_node_config, routing_key,
            target, target_args=None, target_kwargs=None,
    ):
        """ Preload model """
        log.info(
            "[%s] Loading model: %s - %s - %s",
            routing_key, target, target_args, target_kwargs,
        )
        #
        if target_args is None:
            target_args = []
        #
        if target_kwargs is None:
            target_kwargs = {}
        #
        try:
            target_pkg, target_name = target.rsplit(".", 1)
            target_cls = getattr(
                importlib.import_module(target_pkg),
                target_name
            )
            #
            target_model = target_cls(*target_args, **target_kwargs)
        except:  # pylint: disable=W0702
            log.exception("[%s] Failed to load model", routing_key)
            time.sleep(5.0)
            return
        #
        log.info("[%s] Model loaded", routing_key)
        #
        event_node = arbiter.make_event_node(config=event_node_config)
        event_node.start()
        #
        task_node = arbiter.TaskNode(
            event_node,
            multiprocessing_context="threading",
            thread_scan_interval=0.1,
            pool=f"preloaded:{routing_key}",
            task_limit=1,
            ident_prefix=f"process:{routing_key}:",
        )
        #
        def _stream_chunk(stream_id, chunk):
            event_node.emit(
                "stream_event",
                {
                    "stream_id": stream_id,
                    "type": "stream_chunk",
                    "data": chunk,
                },
            )
        #
        def _stream_end(stream_id):
            event_node.emit(
                "stream_event",
                {
                    "stream_id": stream_id,
                    "type": "stream_end",
                    "data": None,
                },
            )
        #
        def _task_invoke(method, args=None, kwargs=None):
            try:
                if args is None:
                    args = []
                #
                if kwargs is None:
                    kwargs = {}
                #
                target_method = getattr(target_model, method)
                result = target_method(*args, **kwargs)
                #
                return result
            finally:
                gc.collect()
                #
                try:
                    import torch  # pylint: disable=C0415,E0401
                    torch.cuda.empty_cache()
                except:  # pylint: disable=W0702
                    pass
        #
        def _task_stream(stream_id, method, args=None, kwargs=None):
            try:
                if args is None:
                    args = []
                #
                if kwargs is None:
                    kwargs = {}
                #
                target_method = getattr(target_model, method)
                #
                for chunk in target_method(*args, **kwargs):
                    _stream_chunk(stream_id, chunk)
            finally:
                _stream_end(stream_id)
                #
                gc.collect()
                #
                try:
                    import torch  # pylint: disable=C0415,E0401
                    torch.cuda.empty_cache()
                except:  # pylint: disable=W0702
                    pass
        #
        task_node.register_task(_task_invoke, "invoke_model")
        task_node.register_task(_task_stream, "stream_model")
        #
        log.info("[%s] Starting processing", routing_key)
        task_node.start(block=True)
