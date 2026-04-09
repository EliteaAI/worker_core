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

import queue

from pylon.core.tools import log  # pylint: disable=E0611,E0401,W0611
from pylon.core.tools import web  # pylint: disable=E0611,E0401,W0611

from arbiter.tasknode.tools import InterruptTaskThread  # pylint: disable=E0611,E0401


class Method:  # pylint: disable=E1101,R0903,W0201
    """
        Method Resource

        self is pointing to current Module instance

        web.method decorator takes zero or one argument: method name
        Note: web.method decorator must be the last decorator (at top)
    """

    @web.method()
    def task_queue_proxy(self, *args, **kwargs):
        """ Run task via queue transparently for caller """
        task_id_queue = None
        task_id = None
        #
        try:
            import tasknode_task  # pylint: disable=E0401,C0415
            #
            task_meta = tasknode_task.meta.copy()
            task_meta["proxy_task_id"] = tasknode_task.id
            #
            task_id_queue = self.task_queue.add(
                name=tasknode_task.name,
                args=args,
                kwargs=kwargs,
                pool="worker",
                meta=task_meta,
            )
            #
            while True:
                try:
                    task_id = task_id_queue.get(timeout=1)
                    break
                except queue.Empty:
                    continue
            #
            return self.task_node_heavy.join_task(task_id)
        #
        except InterruptTaskThread:
            if self.task_queue.debug:
                log.exception("Proxy task cancelled")
            #
            if task_id is not None:
                if self.task_queue.debug:
                    log.info("Stopping subtask: %s", task_id)
                #
                self.task_node_heavy.stop_task(task_id)
            #
            elif task_id_queue is not None:
                if self.task_queue.debug:
                    log.info("Cancelling queued subtask")
                #
                self.task_queue.cancel(task_id_queue)
            #
            return ...
