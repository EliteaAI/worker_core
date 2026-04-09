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
import queue
import importlib
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

    @web.method()
    def invoke_model(  # pylint: disable=R0912,R0913
            self,
            routing_key,
            *,
            method="__call__", method_args=None, method_kwargs=None,
            target=None, target_args=None, target_kwargs=None, target_io_bound=False,
            stream_id=None,
            block=True,
    ):
        """ Invoke model (client) """
        log.info("invoke_model() = %s", routing_key)
        #
        # Case: model was preloaded
        #
        if routing_key in self.preloaded_model_keys:
            if stream_id is None:
                task_name = "invoke_model"
                task_kwargs = {
                    "method": method,
                    "args": method_args,
                    "kwargs": method_kwargs,
                }
            else:
                task_name = "stream_model"
                task_kwargs = {
                    "stream_id": stream_id,
                    "method": method,
                    "args": method_args,
                    "kwargs": method_kwargs,
                }
            #
            id_promise = self.task_queue_preload.add(
                name=task_name,
                kwargs=task_kwargs,
                pool=f"preloaded:{routing_key}",
            )
            #
            while True:
                try:
                    task_id = id_promise.get(timeout=1)
                    break
                except queue.Empty:
                    continue
            #
            log.info("Preloaded task id: %s", task_id)
            #
            if not block:
                return task_id
            #
            return self.task_node_preload.join_task(task_id)
        #
        # Case: requested preloaded model that is not preloaded
        #
        if target is None:
            raise RuntimeError(f"Model not preloaded: {routing_key}")
        #
        # Case: requested streaming for non-preloaded targets
        #
        if stream_id is not None:
            raise RuntimeError(f"Cannot stream: {routing_key}")
        #
        # Case: IO-bound target
        #
        if target_io_bound:
            task_id = self.task_node_light.start_task(
                name="invoke_model_task",
                kwargs={
                    "target": target,
                    "target_args": target_args,
                    "target_kwargs": target_kwargs,
                    "method": method,
                    "method_args": method_args,
                    "method_kwargs": method_kwargs,
                },
                pool="indexer",
            )
            #
            log.info("Light task id: %s", task_id)
            #
            if not block:
                return task_id
            #
            return self.task_node_light.join_task(task_id)
        #
        # Case: CPU-bound target
        #
        id_promise = self.task_queue.add(
            name="invoke_model_task",
            kwargs={
                "target": target,
                "target_args": target_args,
                "target_kwargs": target_kwargs,
                "method": method,
                "method_args": method_args,
                "method_kwargs": method_kwargs,
            },
            pool="worker",
        )
        #
        while True:
            try:
                task_id = id_promise.get(timeout=1)
                break
            except queue.Empty:
                continue
        #
        log.info("Heavy task id: %s", task_id)
        #
        if not block:
            return task_id
        #
        return self.task_node_heavy.join_task(task_id)


    @web.method()
    def invoke_model_task(  # pylint: disable=R0913
            self,
            target, target_args=None, target_kwargs=None,
            method="__call__", method_args=None, method_kwargs=None,
    ):
        """ Invoke model (worker) """
        try:
            if target_args is None:
                target_args = []
            #
            if target_kwargs is None:
                target_kwargs = {}
            #
            target_pkg, target_name = target.rsplit(".", 1)
            target_cls = getattr(
                importlib.import_module(target_pkg),
                target_name
            )
            #
            target_model = target_cls(*target_args, **target_kwargs)
            #
            if method_args is None:
                method_args = []
            #
            if method_kwargs is None:
                method_kwargs = {}
            #
            target_method = getattr(target_model, method)
            result = target_method(*method_args, **method_kwargs)
            #
            gc.collect()
            return result
        except:  # pylint: disable=W0702
            log.debug("invoke_model_task: exception: %s", traceback.format_exc())
            raise
