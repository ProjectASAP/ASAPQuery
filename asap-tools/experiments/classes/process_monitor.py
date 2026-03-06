import multiprocessing
import psutil
import traceback
from typing import List, Any
from classes.ProcessMonitorHook import ProcessMonitorHook, ProcessMetricSnapshot


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
    ):
        super(MyMonitor, self).__init__()
        self.pids_to_monitor = pids_to_monitor
        self.keywords = keywords
        self.pipe = pipe
        self.interval = interval
        self.monitors = monitors
        self.hooks = hooks
        self.include_children = include_children

        assert len(self.pids_to_monitor) == len(self.keywords)

        self.psutil_handles = {pid: psutil.Process(pid) for pid in self.pids_to_monitor}

        self.pid_monitor_map = {}
        for pid, keyword in zip(self.pids_to_monitor, self.keywords):
            self.pid_monitor_map[pid] = {m: [] for m in self.monitors}
            self.pid_monitor_map[pid]["keyword"] = keyword

    def add_child_pid_to_map(self, pid, child_pid):
        self.pid_monitor_map[child_pid] = {m: [] for m in self.monitors}
        self.pid_monitor_map[child_pid]["keyword"] = self.pid_monitor_map[pid][
            "keyword"
        ]

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
        stop = False

        try:
            while True:
                iteration_info = []  # list of process snapshots from this iteration
                for pid, p in self.psutil_handles.items():
                    iteration_info += self.update_pid_monitor_map(p)
                    if self.include_children:
                        for child in p.children(recursive=True):
                            if child.pid not in self.pid_monitor_map:
                                self.add_child_pid_to_map(pid, child.pid)
                            iteration_info += self.update_pid_monitor_map(child)

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
    )
    monitor.start()
    control_pipe.recv()
    return monitor, control_pipe, monitor_pipe


def stop_monitor(monitor, control_pipe, monitor_pipe):
    control_pipe.send("stop")
    can_read = control_pipe.poll(10)
    if can_read:
        monitor_info = control_pipe.recv()
        monitor.join()
    else:
        monitor_info = None
        monitor.terminate()
        monitor.join()
    return monitor_info
