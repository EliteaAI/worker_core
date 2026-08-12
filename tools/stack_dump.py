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

""" Read-only stack dumps for hung tasks """

import collections
import ctypes
import itertools
import os
import signal
import sys
import threading
import traceback
import faulthandler

from pylon.core.tools import log  # pylint: disable=E0611,E0401


DUMP_DIR = "/tmp/tasknode_dumps"

# faulthandler emits a bare traceback with no delimiter, so the parent fences
# each dump with this marker before signalling; that is what makes them splittable.
MARKER_PREFIX = "#--- elitea stack dump "

# This payload rides the event bus back to pylon_main, and a deep recursive
# stack can be enormous.
MAX_DUMP_CHARS = 64 * 1024

# Bounds sink growth across repeated presses.
MAX_DUMPS_RETURNED = 6

SIGNAL_POLL_ATTEMPTS = 100
SIGNAL_POLL_INTERVAL = 0.02

# Runtime scaffolding: identical in every dump, and pushes the interesting
# task frames off the top of the panel.
_NOISE_PATH_FRAGMENTS = (
    "/multiprocessing/process.py",
    "/multiprocessing/popen_fork.py",
    "/multiprocessing/context.py",
    "/threading.py",
    "/arbiter/tasknode/",
)

# A healthy streaming task parks in a blocking socket read between tokens, so
# two identical samples there mean "waiting on a peer", not "hung".
_IO_WAIT_FUNCTIONS = frozenset({
    "recv", "recv_into", "recvfrom", "recvmsg", "sendall", "sendto", "sendmsg",
    "do_handshake", "connect", "accept", "select", "poll", "_poll", "epoll_wait",
})

# 'read'/'wait'-style names are too generic to judge alone; inside these modules
# they are unambiguously a blocking wait on someone else.
_IO_WAIT_PATHS = (
    "/socket.py", "/ssl.py", "/selectors.py",
    "/httpcore/", "/httpx/", "/urllib3/", "/h11/", "/h2/",
)

_fork_hook_installed = False
_child_sink = {}
_sequence = itertools.count(1)

# Threading-mode tasks have no sink to accumulate into, so the previous dump is
# remembered here to keep the two-press diagnostic working for them too.
_thread_history = collections.OrderedDict()

# LRU is the only cleanup: nothing signals task completion here, so entries are
# evicted by age. Bounds worst case at this many dumps of MAX_DUMP_CHARS.
MAX_THREAD_HISTORY = 16


def next_sequence():
    """ Marker id; only needs to be unique within one sink """
    return next(_sequence)


def remember_thread_dump(task_id, body):
    """ Store this dump and return the one from the previous press """
    previous = _thread_history.pop(task_id, None)
    #
    _thread_history[task_id] = body
    while len(_thread_history) > MAX_THREAD_HISTORY:
        _thread_history.popitem(last=False)
    #
    return previous


def _sink_path(pid):
    return os.path.join(DUMP_DIR, f"{pid}.dump")


def _after_fork_in_child():
    """ Re-point this child's faulthandler at its own sink """
    try:
        path = _sink_path(os.getpid())
        sink = open(path, "a+b", buffering=0)  # pylint: disable=R1732,W1514
        faulthandler.register(signum=signal.SIGUSR1, file=sink)
        _child_sink["file"] = sink  # keep a ref; GC would close the fd
        # Unlinked at once: the fd stays parent-readable via /proc/<pid>/fd, but
        # a SIGKILLed child leaves no orphan behind on a PVC-backed /tmp.
        os.unlink(path)
    except:  # pylint: disable=W0702
        pass  # a child that cannot be dumped must still run its task


def install_fork_hook():
    """ Arm per-task dump capture for every future forked task child """
    global _fork_hook_installed  # pylint: disable=W0603
    #
    if _fork_hook_installed:
        return
    #
    try:
        os.makedirs(DUMP_DIR, exist_ok=True)
        os.register_at_fork(after_in_child=_after_fork_in_child)
        _fork_hook_installed = True
        log.info("Task stack dump capture armed (sink: %s)", DUMP_DIR)
    except:  # pylint: disable=W0702
        log.exception("Failed to arm task stack dump capture")


def _find_sink_fd(pid):
    """ Locate this pid's own unlinked sink among its open fds """
    # Matched on the pid-derived name, not the directory: a nested fork inherits
    # its parent's sink fd, and appending there would corrupt the parent's history.
    want = f"{_sink_path(pid)} (deleted)"
    #
    for name in os.listdir(f"/proc/{pid}/fd"):
        try:
            target = os.readlink(f"/proc/{pid}/fd/{name}")
        except OSError:
            continue  # fd closed under us mid-scan
        #
        if target == want:
            return f"/proc/{pid}/fd/{name}"
    #
    return None


def _write_marker(sink_path, sequence):
    """ Fence the dump that SIGUSR1 is about to append """
    with open(sink_path, "ab", buffering=0) as sink:
        sink.write(f"\n{MARKER_PREFIX}{sequence}\n".encode("utf-8"))


def _read_sink(sink_path):
    try:
        with open(sink_path, "rb") as sink:
            return sink.read().decode("utf-8", "replace")
    except OSError:
        return ""


def _split_dumps(text):
    """ Split a marker-fenced sink into dump bodies, oldest first """
    chunks = []
    #
    for raw in text.split(MARKER_PREFIX)[1:]:  # [0] is pre-marker noise
        body = raw.split("\n", 1)[1] if "\n" in raw else ""
        body = body.strip()
        if body:
            chunks.append(body[:MAX_DUMP_CHARS])
    #
    return chunks


def _frame_lines(dump_text):
    """ Just the code positions, for comparing two dumps """
    return [
        line.strip() for line in dump_text.splitlines()
        if line.strip().startswith(("File \"", "  File \""))
    ]


def significant_frames(dump_text):
    """ Frames with runtime scaffolding dropped; falls back to all frames """
    interesting = [
        line for line in _frame_lines(dump_text)
        if not any(fragment in line for fragment in _NOISE_PATH_FRAGMENTS)
    ]
    #
    return interesting if interesting else _frame_lines(dump_text)


def task_section(dump_text):
    """ Just the thread running the task, out of an all-threads dump """
    # faulthandler dumps every thread; idle pool workers churn on their own and
    # would otherwise show up as the task making progress.
    if "Current thread" in dump_text:
        return "Current thread" + dump_text.split("Current thread")[-1]
    #
    return dump_text


def _leaf_frame(dump_text):
    """ Innermost frame of the thread running the task """
    dump_text = task_section(dump_text)
    frames = _frame_lines(dump_text)
    if not frames:
        return ""
    #
    # faulthandler emits leaf-first and says so in its header; format_stack is leaf-last.
    if "most recent call first" in dump_text:
        return frames[0]
    #
    return frames[-1]


def waiting_on_io(dump_text):
    """ True when the innermost frame is a blocking wait on a peer """
    leaf = _leaf_frame(dump_text)
    if not leaf:
        return False
    #
    # 'File "<path>", line N in <func>' — faulthandler and traceback agree here.
    function = leaf.rsplit(" in ", 1)[-1].strip() if " in " in leaf else ""
    path = leaf.split("\"")[1] if leaf.count("\"") >= 2 else ""
    #
    if function in _IO_WAIT_FUNCTIONS:
        return True
    #
    return any(fragment in path for fragment in _IO_WAIT_PATHS)


def compare_dumps(previous, current):
    """ Classify progress between two dumps of the same task """
    # This is what the two presses are actually for, so compute it here rather
    # than making an operator eyeball two stacks.
    if not previous or not current:
        return "unknown"
    #
    previous, current = task_section(previous), task_section(current)
    #
    if _frame_lines(previous) == _frame_lines(current):
        # Same stack parked in a socket read is a slow peer, not a wedged task.
        return "waiting_on_io" if waiting_on_io(current) else "stuck"
    #
    if significant_frames(previous) == significant_frames(current):
        return "stuck_in_library"
    #
    return "spinning"


def dump_forked_task(pid, sequence):
    """ SIGUSR1 a forked task child and read back what it wrote; (dumps, error) """
    if not os.path.isdir(f"/proc/{pid}"):
        return [], f"process {pid} is gone"
    #
    sink_path = _find_sink_fd(pid)
    if sink_path is None:
        return [], (
            f"no dump sink on pid {pid} — it was forked before capture was armed"
        )
    #
    try:
        before = os.path.getsize(sink_path)
        _write_marker(sink_path, sequence)
        os.kill(pid, signal.SIGUSR1)
    except OSError as exc:
        return [], f"could not signal pid {pid}: {exc}"
    #
    # faulthandler writes from the signal handler, so the append is not visible
    # the instant kill() returns; a wedged process still answers (that is the point).
    marker_end = before + len(MARKER_PREFIX) + 8
    idle = threading.Event()  # gevent-friendly sleep
    settled = -1
    for _ in range(SIGNAL_POLL_ATTEMPTS):
        size = os.path.getsize(sink_path)
        # A multi-thread dump is written frame by frame, so "grew past the marker"
        # is not "finished"; wait for the size to stop moving or we read a torn stack.
        if size > marker_end and size == settled:
            break
        settled = size
        idle.wait(SIGNAL_POLL_INTERVAL)
    #
    dumps = _split_dumps(_read_sink(sink_path))[-MAX_DUMPS_RETURNED:]
    #
    if not dumps:
        return [], (
            f"pid {pid} did not answer SIGUSR1 — it may be stopped (T state) "
            "or blocked in a non-interruptible syscall"
        )
    #
    return dumps, None


def dump_task_thread(thread, greenlet_runtime):
    """ Stack of one task thread, read live out of this interpreter """
    # No signal: signalling a threading-mode pool would dump every thread in the
    # process. Under gevent the 'thread' is a greenlet, absent from _current_frames().
    if thread is None or not thread.is_alive():
        return None, "task thread is not alive"
    #
    frame = None
    #
    if greenlet_runtime:
        try:
            greenlet = ctypes.cast(thread.ident, ctypes.py_object).value
            frame = getattr(greenlet, "gr_frame", None)
        except:  # pylint: disable=W0702
            frame = None
        #
        if frame is None:
            return None, "greenlet is not suspended in Python code"
    else:
        frame = sys._current_frames().get(thread.ident)  # pylint: disable=W0212
        if frame is None:
            return None, "thread has no live Python frame"
    #
    try:
        body = "".join(traceback.format_stack(frame)).strip()
    except:  # pylint: disable=W0702
        return None, "could not format thread stack"
    #
    return body[:MAX_DUMP_CHARS], None
