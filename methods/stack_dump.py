#!/usr/bin/python3
# coding=utf-8

#   Copyright 2026 EPAM Systems
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

import os

from pylon.core.tools import log  # pylint: disable=E0611,E0401,W0611
from pylon.core.tools import web  # pylint: disable=E0611,E0401,W0611

from tools import context  # pylint: disable=E0401

from ..tools import stack_dump


# Every TaskNode in this pylon that actually runs tasks. Deliberately not
# bootstrap's TASK_TARGETS: that repo is shared and pinned, this list is local.
LOCAL_TASK_NODES = [
    ("worker_core", "task_node_heavy"),
    ("worker_core", "task_node_preload"),
    ("worker_core", "task_node_light"),
    ("indexer_worker", "agent_task_node"),
    ("indexer_worker", "index_task_node"),
    ("indexer_worker", "index_maintenance_task_node"),
]


class Method:  # pylint: disable=E1101,R0903,W0201
    """
        Method Resource

        self is pointing to current Module instance

        web.method decorator takes zero or one argument: method name
        Note: web.method decorator must be the last decorator (at top)
    """

    @web.method()
    def stack_dump_event_request(self, event, data):
        """ Event: dump the stack of a task this pylon owns """
        # Broadcast to every node; each answers only for tasks in its own
        # running_tasks, the same ownership rule as task_stop_request.
        _ = event
        #
        task_id = data.get("task_id")
        if not task_id:
            return
        #
        located = _find_running_task(self.context.module_manager, task_id)
        if located is None:
            return
        #
        node_label, task_node, task_data = located
        #
        log.info("Dumping stack of task %s on %s", task_id, node_label)
        #
        try:
            reply = _collect_dump(task_id, task_node, task_data)
        except Exception as exc:  # pylint: disable=W0703
            log.exception("Stack dump failed for task %s", task_id)
            reply = {"ok": False, "error": f"dump failed: {exc}"}
        #
        reply.update({
            "request_id": data.get("request_id"),
            "task_id": task_id,
            "pylon_id": context.id,
            "node": node_label,
        })
        #
        self.event_node.emit("task_dump_reply", reply)


def _find_running_task(module_manager, task_id):
    """ Find which local TaskNode is running this task """
    for plugin_name, node_name in LOCAL_TASK_NODES:
        if plugin_name not in module_manager.modules:
            continue
        #
        plugin = module_manager.modules[plugin_name].module
        if plugin is None:
            continue
        #
        task_node = getattr(plugin, node_name, None)
        if task_node is None or not task_node.started:
            continue
        #
        with task_node.lock:
            task_data = task_node.running_tasks.get(task_id)
            if task_data is None:
                continue
            #
            task_data = task_data.copy()
        #
        return f"{plugin_name}.{node_name}", task_node, task_data
    #
    return None


def _collect_dump(task_id, task_node, task_data):
    """ Produce the dump payload for one running task """
    process = task_data.get("process")
    #
    if process is not None:
        return _collect_process_dump(process)
    #
    return _collect_thread_dump(task_id, task_node, task_data.get("thread"))


def _collect_process_dump(process):
    """ Forked task: signal it and read back its own sink """
    if not process.is_alive():
        return {"ok": False, "mode": "process", "error": "task process is no longer alive"}
    #
    pid = process.pid
    if pid is None:
        return {"ok": False, "mode": "process", "error": "task process has no pid yet"}
    #
    dumps, error = stack_dump.dump_forked_task(pid, stack_dump.next_sequence())
    #
    if error is not None:
        return {"ok": False, "mode": "process", "pid": pid, "error": error}
    #
    current = dumps[-1]
    previous = dumps[-2] if len(dumps) > 1 else None
    #
    return {
        "ok": True,
        "mode": "process",
        "pid": pid,
        "dump": current,
        "previous_dump": previous,
        "dump_count": len(dumps),
        "verdict": stack_dump.compare_dumps(previous, current),
    }


def _collect_thread_dump(task_id, task_node, thread):
    """ Threading-mode task: read the one thread's frame, no signal """
    body, error = stack_dump.dump_task_thread(thread, task_node.gevent_runtime)
    #
    if error is not None:
        return {"ok": False, "mode": "thread", "error": error}
    #
    previous = stack_dump.remember_thread_dump(task_id, body)
    #
    return {
        "ok": True,
        "mode": "thread",
        "pid": os.getpid(),
        "dump": body,
        "previous_dump": previous,
        "dump_count": 2 if previous else 1,
        "verdict": stack_dump.compare_dumps(previous, body),
    }
