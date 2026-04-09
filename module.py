#!/usr/bin/python3
# coding=utf-8
# pylint: disable=C0413

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

""" Module """

import os
import json
import queue
import resource

import websocket  # pylint: disable=E0401
import engineio.client  # pylint: disable=E0401
engineio.client.websocket = websocket

from pylon.core.tools import log  # pylint: disable=E0611,E0401
from pylon.core.tools import module  # pylint: disable=E0611,E0401

import arbiter  # pylint: disable=E0401

from .tools.rpc import wrap_exceptions


class Module(module.ModuleModel):  # pylint: disable=R0902
    """ Pylon module """

    def __init__(self, context, descriptor):
        self.context = context
        self.descriptor = descriptor
        #
        self.event_node_config = None
        self.event_node = None
        #
        self.rpc_node = None
        #
        self.tasknode_tmp = None
        #
        self.preload_config = []
        #
        self.task_node_preload = None
        self.preloaded_model_keys = set()
        self.preloaded_model_tasks = {}
        #
        self.preloaded_models_enabled = True
        self.external_models_enabled = True
        self.indexer_tasks_enabled = False
        #
        self.task_node_light = None
        self.task_node_heavy = None
        #
        self.task_queue_cls = arbiter.TaskQueue
        self.task_queue_preload = None
        self.task_queue = None
        #
        self.wrap_exceptions = wrap_exceptions
        #
        # Task tracing wrapper (set in init if tracing plugin available)
        self._task_wrapper = None

    def _wrap_task(self, handler, task_name: str):
        """Wrap a task handler with lazy tracing (checks for tracing at execution time)."""
        import functools

        @functools.wraps(handler)
        def lazy_traced_handler(*args, **kwargs):
            # Try to get tracer at execution time (tracing plugin may be loaded now)
            if self._task_wrapper is None:
                self._init_task_tracing()

            if self._task_wrapper is not None:
                try:
                    # Apply the wrapper and call
                    wrapped = self._task_wrapper(task_name)(handler)
                    return wrapped(*args, **kwargs)
                except Exception as e:
                    log.debug(f"Task tracing failed for {task_name}: {e}")

            # Fallback: call without tracing
            return handler(*args, **kwargs)

        return lazy_traced_handler

    def _init_task_tracing(self):
        """Initialize task tracing wrapper from tracing plugin."""
        try:
            from tools import this
            tracing_module = this.for_module("tracing").module
            if tracing_module.enabled:
                self._task_wrapper = tracing_module.get_task_wrapper()
                if self._task_wrapper:
                    log.info("Task tracing enabled for worker_core tasks")
        except Exception as e:
            log.debug(f"Task tracing not available: {e}")

    def _patch_task_registration(self):
        """Patch arbiter TaskNode to auto-wrap registered tasks with tracing (lazy at execution)."""
        import arbiter
        import functools

        original_register = arbiter.TaskNode.register_task
        worker_core_self = self

        def traced_register_task(task_node_self, handler, name, *args, **kwargs):
            """Wrapper around TaskNode.register_task that adds lazy tracing at execution time."""

            @functools.wraps(handler)
            def lazy_traced_handler(*task_args, **task_kwargs):
                """Execute handler with tracing if available (checked at execution time)."""
                # Try to get wrapper at execution time (tracing should be loaded by now)
                if worker_core_self._task_wrapper is None:
                    worker_core_self._init_task_tracing()

                if worker_core_self._task_wrapper is not None:
                    try:
                        # Apply tracing wrapper at execution time
                        traced_fn = worker_core_self._task_wrapper(name)(handler)
                        return traced_fn(*task_args, **task_kwargs)
                    except Exception as e:
                        log.debug(f"Task tracing failed for {name}: {e}")

                # Fallback: execute without tracing
                return handler(*task_args, **task_kwargs)

            # Register the lazy wrapper instead of the original handler
            return original_register(task_node_self, lazy_traced_handler, name, *args, **kwargs)

        arbiter.TaskNode.register_task = traced_register_task
        log.info("TaskNode.register_task patched for execution-time task tracing")

    def preload(self):
        """ Preload handler """
        if "TIKTOKEN_CACHE_DIR" in os.environ:
            log.info("Preloading Tiktoken bundle")
            #
            tiktoken_cache_dir = os.environ["TIKTOKEN_CACHE_DIR"]
            #
            os.makedirs(tiktoken_cache_dir, exist_ok=True)
            #
            try:
                from tools import this  # pylint: disable=E0401,C0415
                #
                def _install_needed(*_args, **_kwargs):
                    try:
                        dir_entries = [
                            item for item in os.listdir(tiktoken_cache_dir)
                            if not item.startswith(".")
                        ]
                        return len(dir_entries) == 0
                    except:  # pylint: disable=W0702
                        return True
                #
                this.for_module("bootstrap").module.get_bundle(
                    "tiktoken-encodings.tar.gz",
                    install_needed=_install_needed,
                    processing="tar_extract",
                    extract_target=tiktoken_cache_dir,
                    extract_cleanup=False,
                )
                #
                log.info("Preloaded Tiktoken bundle")
            except:  # pylint: disable=W0702
                log.exception("Failed to preload Tiktoken bundle")
        #
        self.descriptor.register_tool("worker_core", self)

    def init(self):  # pylint: disable=R0912,R0915
        """ Init module """
        log.info("Initializing module")
        # Init
        self.descriptor.init_all()
        # Patch task registration for tracing (must be early, before other plugins register tasks)
        self._patch_task_registration()
        # Disable core file generation
        log.info("Disabling core file generation")
        resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
        # Bundles
        if "TIKTOKEN_CACHE_DIR" in os.environ:
            tiktoken_cache_dir = os.environ["TIKTOKEN_CACHE_DIR"]
            #
            os.makedirs(tiktoken_cache_dir, exist_ok=True)
            #
            try:
                from tools import this  # pylint: disable=E0401,C0415
                #
                def _install_needed(*_args, **_kwargs):
                    try:
                        dir_entries = [
                            item for item in os.listdir(tiktoken_cache_dir)
                            if not item.startswith(".")
                        ]
                        return len(dir_entries) == 0
                    except:  # pylint: disable=W0702
                        return True
                #
                this.for_module("bootstrap").module.get_bundle(
                    "tiktoken-encodings.tar.gz",
                    install_needed=_install_needed,
                    processing="tar_extract",
                    extract_target=tiktoken_cache_dir,
                    extract_cleanup=False,
                )
                log.info("Using Tiktoken bundle")
            except:  # pylint: disable=W0702
                pass
        # EventNode
        self.event_node_config = self.get_event_node_config()
        self.event_node = arbiter.make_event_node(
            config=self.event_node_config,
        )
        self.event_node.start()
        self.event_node.subscribe("stream_event", self.on_stream_event)
        # RPCNode
        self.rpc_node = arbiter.RpcNode(
            self.event_node,
            id_prefix="indexer_",
            proxy_timeout=5,
        )
        self.rpc_node.start()
        # Bootstrap
        self.event_node.subscribe("bootstrap_runtime_update", self.i2p_bootstrap_runtime_update)
        # Enable-switches
        self.preloaded_models_enabled = self.descriptor.config.get("preloaded_models_enabled", True)
        self.external_models_enabled = self.descriptor.config.get("external_models_enabled", True)
        self.indexer_tasks_enabled = self.descriptor.config.get("indexer_tasks_enabled", False)
        # GPU?
        torch_default_device = self.descriptor.config.get("torch_default_device", None)
        if torch_default_device is not None:
            try:
                import torch  # pylint: disable=E0401,C0415
                torch.set_default_device(torch_default_device)
            except:  # pylint: disable=W0702
                pass
        # TaskNode commons
        self.tasknode_tmp = self.get_tasknode_tmp()
        # TaskNode: preload
        preload_config = self.descriptor.config.get("preloaded_models", None)
        #
        if preload_config is None:
            log.info("preloaded_models is not set in config, querying main pylon")
            try:
                while True:
                    try:
                        log.info("Pinging main to check if it is there")
                        ping_ok = self.rpc_node.proxy.restricted_ping()
                        if ping_ok is True:
                            break
                    except queue.Empty:
                        continue
                #
                log.info("Getting config from administration")
                preload_config = json.loads(
                    self.rpc_node.proxy.restricted_get_admin_secret(
                        "ai_preloaded_models"
                    )
                )
            except:  # pylint: disable=W0702
                preload_config = None
        #
        if preload_config is None:
            preload_config = []
        #
        self.preload_config = preload_config
        #
        self.task_node_preload = arbiter.TaskNode(
            self.event_node,
            pool=f"preload:{self.context.id}",
            task_limit=None \
                if self.preloaded_models_enabled else 0,
            start_attempts=1,  # 'Client' attempts and retries are made by TaskQueue
            ident_prefix="preload_",
            multiprocessing_context="fork",
            kill_on_stop=False,
            task_retention_period=3600,
            housekeeping_interval=60,
            start_max_wait=3,
            query_wait=3,
            watcher_max_wait=3,
            stop_node_task_wait=3,
            result_max_wait=3,
            tmp_path=self.tasknode_tmp,
            result_transport="files",
        )
        self.task_node_preload.start()
        #
        if self.preloaded_models_enabled:
            self.task_node_preload.register_task(
                self._wrap_task(self.preload_model, "preload_model"), "preload_model"
            )
        #
        self.task_queue_preload = arbiter.TaskQueue(
            self.task_node_preload,
            debug=self.descriptor.config.get("task_queue_debug", False),
        )
        self.task_queue_preload.start()
        # TaskNode: heavy (for non-preloaded models)
        heavy_node_enabled = self.external_models_enabled or self.indexer_tasks_enabled
        #
        self.task_node_heavy = arbiter.TaskNode(
            self.event_node,
            pool="worker",
            task_limit=self.descriptor.config.get("task_limit_heavy", None) \
                if heavy_node_enabled else 0,
            start_attempts=1,  # Attempts and retries are made by TaskQueue
            ident_prefix="worker_",
            multiprocessing_context="fork",
            kill_on_stop=False,
            task_retention_period=3600,
            housekeeping_interval=60,
            start_max_wait=3,
            query_wait=3,
            watcher_max_wait=3,
            stop_node_task_wait=3,
            result_max_wait=3,
            tmp_path=self.tasknode_tmp,
            result_transport="files",
        )
        self.task_node_heavy.start()
        #
        if self.external_models_enabled:
            self.task_node_heavy.register_task(
                self._wrap_task(self.invoke_model_task, "invoke_model_task"), "invoke_model_task"
            )
        # TaskNode: light (main client entrypoint; for router/proxy/IO tasks)
        light_node_enabled = self.external_models_enabled or self.indexer_tasks_enabled
        #
        self.task_node_light = arbiter.TaskNode(
            self.event_node,
            pool="indexer",
            task_limit=self.descriptor.config.get("task_limit_light", None) \
                if light_node_enabled else 0,
            ident_prefix="indexer_light_",
            multiprocessing_context="threading",
            task_retention_period=3600,
            housekeeping_interval=60,
            thread_scan_interval=0.1,
            start_max_wait=3,
            query_wait=3,
            watcher_max_wait=3,
            stop_node_task_wait=3,
            result_max_wait=3,
            result_transport="memory",
        )
        self.task_node_light.start()
        #
        if self.external_models_enabled:
            self.task_node_light.register_task(
                self._wrap_task(self.invoke_model_task, "invoke_model_task"), "invoke_model_task"
            )
            self.task_node_light.register_task(
                self._wrap_task(self.invoke_model, "invoke_model"), "invoke_model"
            )
        # TaskQueue
        self.task_queue = arbiter.TaskQueue(
            self.task_node_heavy,
            debug=self.descriptor.config.get("task_queue_debug", False),
        )
        self.task_queue.start()
        # Tool
        self.descriptor.register_tool("worker_core", self)
        # Postgres proxy
        postgres_proxy = self.descriptor.config.get("postgres_proxy", None)
        #
        if postgres_proxy:
            from .tools.postgres import start_postgres_proxy
            start_postgres_proxy(
                bind=postgres_proxy.get("bind", "0.0.0.0:5432"),
                remote=postgres_proxy.get("remote"),
                scope=postgres_proxy.get("scope", "https://ossrdbms-aad.database.windows.net/.default"),
            )

    def ready(self):
        """ Ready callback """
        for item in self.preload_config:
            routing_key = item.get("routing_key", None)
            target = item.get("target", None)
            target_args = item.get("target_args", None)
            target_kwargs = item.get("target_kwargs", None)
            workers = item.get("workers", 1)
            #
            if routing_key is None or target is None:
                continue
            #
            self.preloaded_model_keys.add(routing_key)
            #
            if not self.preloaded_models_enabled:
                continue
            #
            task_config = {
                "event_node_config": self.event_node_config,
                "routing_key": routing_key,
                "target": target,
                "target_args": target_args,
                "target_kwargs": target_kwargs,
            }
            #
            for _ in range(workers):
                preload_task_id = self.task_node_preload.start_task(
                    "preload_model",
                    kwargs=task_config,
                    pool=f"preload:{self.context.id}",
                    durable=True,
                )
                #
                if routing_key not in self.preloaded_model_tasks:
                    self.preloaded_model_tasks[routing_key] = []
                #
                self.preloaded_model_tasks[routing_key].append(preload_task_id)
                #
                log.info("Started preload task: %s -> %s", routing_key, preload_task_id)

    def deinit(self):
        """ De-init module """
        log.info("De-initializing module")
        # Tool
        self.descriptor.unregister_tool("worker_core")
        # Tasks
        if self.external_models_enabled:
            self.task_node_light.unregister_task(self.invoke_model, "invoke_model")
            self.task_node_light.unregister_task(self.invoke_model_task, "invoke_model_task")
            self.task_node_heavy.unregister_task(self.invoke_model_task, "invoke_model_task")
        #
        if self.preloaded_models_enabled:
            self.task_node_preload.unregister_task(self.preload_model, "preload_model")
        # RPCNode
        self.rpc_node.stop()
        # TaskQueue
        self.task_queue.stop()
        self.task_queue_preload.stop()
        # TaskNode
        self.task_node_light.stop()
        self.task_node_heavy.stop()
        self.task_node_preload.stop()
        # Bootstrap
        self.event_node.unsubscribe("bootstrap_runtime_update", self.i2p_bootstrap_runtime_update)
        # EventNode
        self.event_node.unsubscribe("stream_event", self.on_stream_event)
        self.event_node.stop()
        # De-init
        self.descriptor.deinit_all()
