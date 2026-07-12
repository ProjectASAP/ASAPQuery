import multiprocessing
import time
import psutil
import traceback
from typing import List, Any, Optional
from classes.ProcessMonitorHook import ProcessMonitorHook, ProcessMetricSnapshot

_PRECOMPUTE_THREAD_PREFIX = "pc-worker"


def _read_thread_cpu(pid: int) -> dict:
    """
    Returns {tid: (thread_name, cpu_seconds)} for all threads of pid.
    cpu_seconds = user_time + system_time from psutil; name from /proc/[pid]/task/[tid]/comm.
    Silently skips threads that disappear mid-read.
    """
    result = {}
    try:
        threads = psutil.Process(pid).threads()
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        return result
    for t in threads:
        try:
            with open(f"/proc/{pid}/task/{t.id}/comm") as f:
                name = f.read().strip()
            result[t.id] = (name, t.user_time + t.system_time)
        except (FileNotFoundError, psutil.NoSuchProcess):
            pass
    return result


class MyMonitor(multiprocessing.Process):
    def __init__(
        self,
        pids_to_monitor,
        keywords,
        pipe,
        interval,
        monitors,
        hooks: List[ProcessMonitorHook],
        include_children=False,
        thread_attribution_keyword: Optional[str] = None,
    ):
        super(MyMonitor, self).__init__()
        self.pids_to_monitor = pids_to_monitor
        self.keywords = keywords
        self.pipe = pipe
        self.interval = interval
        self.monitors = monitors
        self.hooks = hooks
        self.include_children = include_children
        self.thread_attribution_keyword = thread_attribution_keyword

        assert len(self.pids_to_monitor) == len(self.keywords)

        self.psutil_handles = {pid: psutil.Process(pid) for pid in self.pids_to_monitor}
        # children() returns a fresh Process object each call, which would reset
        # cpu_percent()'s internal delta tracking every poll. Cache one handle per
        # child pid so cpu_percent() deltas accumulate across polls like they do
        # for the seed pids above.
        self.child_handles = {}

        self.pid_monitor_map = {}
        for pid, keyword in zip(self.pids_to_monitor, self.keywords):
            self.pid_monitor_map[pid] = {m: [] for m in self.monitors}
            self.pid_monitor_map[pid]["keyword"] = keyword

        if self.thread_attribution_keyword is not None:
            self._prev_thread_jiffies: dict = {}
            self._prev_poll_monotonic: float = 0.0
            for pid, keyword in zip(self.pids_to_monitor, self.keywords):
                if keyword == self.thread_attribution_keyword:
                    self.pid_monitor_map[pid]["precompute_cpu_percent"] = []
                    self.pid_monitor_map[pid]["query_cpu_percent"] = []

    def add_child_pid_to_map(self, pid, child_pid):
        self.pid_monitor_map[child_pid] = {m: [] for m in self.monitors}
        keyword = self.pid_monitor_map[pid]["keyword"]
        self.pid_monitor_map[child_pid]["keyword"] = keyword
        if (
            self.thread_attribution_keyword is not None
            and keyword == self.thread_attribution_keyword
        ):
            self.pid_monitor_map[child_pid]["precompute_cpu_percent"] = []
            self.pid_monitor_map[child_pid]["query_cpu_percent"] = []

    def init_hooks(self):
        """
        Initialize all process monitor hooks, e.g. starting exporter servers, etc
        """
        if self.hooks is not None and len(self.hooks) > 0:
            for hook in self.hooks:
                hook.init()
        return

    # TODO Determine whether there should be ability to update certain hooks either
    #      while updating pid monitor map (i.e. per process basis), after updating
    #      entire process map, or both
    def update_hooks(self, value: Any):
        """
        Update all process monitor hooks using the given value
        """
        if self.hooks is not None and len(self.hooks) > 0:
            for hook in self.hooks:
                hook.update(value)
        return

    def close_hooks(self):
        """
        Cleanup any resources associated with process monitor hooks
        """
        if self.hooks is not None and len(self.hooks) > 0:
            for hook in self.hooks:
                hook.close()
        return

    def _compute_thread_group_cpu(self, pid: int, elapsed: float):
        """
        Reads current per-thread CPU seconds for pid, diffs against previous snapshot,
        and appends precompute_cpu_percent / query_cpu_percent to pid_monitor_map.

        CPU% is on the same scale as psutil's cpu_percent: can exceed 100% on
        multi-core systems (e.g. 2 fully loaded cores → ~200%).
        """
        current = _read_thread_cpu(pid)
        prev = self._prev_thread_jiffies.get(pid, {})

        if not prev:
            self._prev_thread_jiffies[pid] = current
            self.pid_monitor_map[pid]["precompute_cpu_percent"].append(0.0)
            self.pid_monitor_map[pid]["query_cpu_percent"].append(0.0)
            return

        precompute_seconds = 0.0
        query_seconds = 0.0

        for tid, (name, cpu_secs) in current.items():
            prev_secs = prev.get(tid, (None, 0.0))[1]
            delta = max(0.0, cpu_secs - prev_secs)
            if name.startswith(_PRECOMPUTE_THREAD_PREFIX):
                precompute_seconds += delta
            else:
                query_seconds += delta

        if elapsed > 0:
            precompute_pct = (precompute_seconds / elapsed) * 100.0
            query_pct = (query_seconds / elapsed) * 100.0
        else:
            precompute_pct = 0.0
            query_pct = 0.0

        self.pid_monitor_map[pid]["precompute_cpu_percent"].append(precompute_pct)
        self.pid_monitor_map[pid]["query_cpu_percent"].append(query_pct)
        self._prev_thread_jiffies[pid] = current

    def update_pid_monitor_map(self, p) -> List[ProcessMetricSnapshot]:
        # if p.pid not in self.pid_monitor_map:
        #     self.pid_monitor_map[p.pid] = {m: [] for m in self.monitors}
        iteration_info = []
        measurement = p.as_dict(attrs=self.monitors)
        for monitor in self.monitors:
            value = None
            if monitor == "memory_info":
                value = measurement[monitor].rss
                self.pid_monitor_map[p.pid][monitor].append(value)
            else:
                value = measurement[monitor]
                self.pid_monitor_map[p.pid][monitor].append(value)

            snapshot = ProcessMetricSnapshot(
                p.pid, value, self.pid_monitor_map[p.pid]["keyword"], monitor
            )
            iteration_info.append(snapshot)

        return iteration_info

    def run(self):
        # NOTE: Possibility of init() (and close()) being called more than once if multiple
        #       processes get started up that were passed the same reference
        #       of the list of hooks
        self.init_hooks()
        self.pipe.send("ready")

        if self.thread_attribution_keyword is not None:
            self._prev_poll_monotonic = time.monotonic()
            for pid, keyword in zip(self.pids_to_monitor, self.keywords):
                if keyword == self.thread_attribution_keyword:
                    self._prev_thread_jiffies[pid] = _read_thread_cpu(pid)

        try:
            while True:
                if self.pipe.poll(0):
                    break

                if self.thread_attribution_keyword is not None:
                    now = time.monotonic()
                    elapsed = now - self._prev_poll_monotonic
                    self._prev_poll_monotonic = now

                iteration_info = []
                stop_requested = False
                for pid, p in self.psutil_handles.items():
                    if self.pipe.poll(0):
                        stop_requested = True
                        break
                    iteration_info += self.update_pid_monitor_map(p)
                    if (
                        self.thread_attribution_keyword is not None
                        and self.pid_monitor_map[pid]["keyword"]
                        == self.thread_attribution_keyword
                    ):
                        self._compute_thread_group_cpu(pid, elapsed)
                    if self.include_children:
                        for child in p.children(recursive=True):
                            if self.pipe.poll(0):
                                stop_requested = True
                                break
                            if child.pid not in self.pid_monitor_map:
                                self.add_child_pid_to_map(pid, child.pid)
                                self.child_handles[child.pid] = child
                            handle = self.child_handles[child.pid]
                            iteration_info += self.update_pid_monitor_map(handle)
                            if (
                                self.thread_attribution_keyword is not None
                                and self.pid_monitor_map[handle.pid]["keyword"]
                                == self.thread_attribution_keyword
                            ):
                                self._compute_thread_group_cpu(handle.pid, elapsed)
                        if stop_requested:
                            break

                if stop_requested:
                    break

                self.update_hooks(iteration_info)
                stop = self.pipe.poll(self.interval)
                if stop:
                    break

            self.pipe.send(self.pid_monitor_map)
            self.close_hooks()

        except Exception as e:
            print(f"Error in monitor process: {e}")
            print(traceback.format_exc())
            self.close_hooks()
            self.pipe.close()


def start_monitor(
    pids_to_monitor,
    keywords,
    monitoring_interval,
    monitor_metrics,
    include_children,
    hooks: List[ProcessMonitorHook],
    thread_attribution_keyword: Optional[str] = None,
):
    control_pipe, monitor_pipe = multiprocessing.Pipe()
    monitor = MyMonitor(
        pids_to_monitor,
        keywords,
        monitor_pipe,
        monitoring_interval,
        monitor_metrics,
        hooks,
        include_children=include_children,
        thread_attribution_keyword=thread_attribution_keyword,
    )
    monitor.start()
    control_pipe.recv()
    return monitor, control_pipe, monitor_pipe


def stop_monitor(monitor, control_pipe, monitor_pipe, timeout=30):
    control_pipe.send("stop")
    can_read = control_pipe.poll(timeout)
    if can_read:
        monitor_info = control_pipe.recv()
        monitor.join(timeout=10)
    else:
        monitor_info = None
        monitor.terminate()
        monitor.join(timeout=10)
    return monitor_info
