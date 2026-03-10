from abc import ABC, abstractmethod
from typing import Any, Optional


class ProcessMonitorHook(ABC):
    """
    Abstract parent class for any hooks in process_monitor
    """

    @abstractmethod
    def init(self):
        pass

    @abstractmethod
    def update(self, value: Any):
        pass

    @abstractmethod
    def close(self):
        pass


class ProcessMetricSnapshot:
    """
    Class for providing hooks with a consistent format for a single measurement
    for a single process
    """

    def __init__(
        self,
        pid: int,
        value: Any,
        keyword: Optional[str] = None,
        monitor_name: Optional[str] = None,
    ):
        self.pid = pid
        self.keyword = keyword
        self.monitor_name = monitor_name
        self.value = value
