import time
from datetime import datetime, timezone
from typing import Optional, Set
from dataclasses import dataclass

from jinja2 import Environment, BaseLoader, TemplateSyntaxError, UndefinedError


@dataclass
class TimeRange:
    """Represents a query time range with Unix timestamps."""

    start_time: int  # Unix timestamp (seconds)
    end_time: int  # Unix timestamp (seconds)

    @property
    def start_datetime(self) -> str:
        """ISO format datetime string for start (UTC)."""
        return datetime.fromtimestamp(self.start_time, tz=timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S"
        )

    @property
    def end_datetime(self) -> str:
        """ISO format datetime string for end (UTC)."""
        return datetime.fromtimestamp(self.end_time, tz=timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S"
        )

    @property
    def start_time_ms(self) -> int:
        """Start time in milliseconds."""
        return self.start_time * 1000

    @property
    def end_time_ms(self) -> int:
        """End time in milliseconds."""
        return self.end_time * 1000


class QueryTemplate:
    """
    Handles Jinja2 template variable substitution in queries.

    Supported variables:
        {{ start_time }}       - Unix timestamp in seconds (int)
        {{ end_time }}         - Unix timestamp in seconds (int)
        {{ start_time_ms }}    - Unix timestamp in milliseconds (int)
        {{ end_time_ms }}      - Unix timestamp in milliseconds (int)
        {{ start_datetime }}   - ISO datetime string (e.g., '2024-01-16 12:00:00')
        {{ end_datetime }}     - ISO datetime string (e.g., '2024-01-16 12:01:00')

    Example usage:
        template = QueryTemplate(
            "SELECT * FROM metrics WHERE ts >= {{ start_time }} AND ts < {{ end_time }}"
        )
        time_range = TimeRange(start_time=1705420800, end_time=1705420860)
        query = template.render(time_range)
        # Result: "SELECT * FROM metrics WHERE ts >= 1705420800 AND ts < 1705420860"
    """

    SUPPORTED_VARS = {
        "start_time",
        "end_time",
        "start_time_ms",
        "end_time_ms",
        "start_datetime",
        "end_datetime",
    }

    def __init__(self, template: str):
        """
        Initialize with a query template.

        Args:
            template: Query string potentially containing {{ variable }} placeholders

        Raises:
            ValueError: If template has syntax errors
        """
        self.template_str = template
        self._env = Environment(loader=BaseLoader(), autoescape=False)

        try:
            self._template = self._env.from_string(template)
        except TemplateSyntaxError as e:
            raise ValueError(f"Invalid template syntax: {e}")

        self._variables = self._extract_variables()

    def _extract_variables(self) -> Set[str]:
        """Extract all template variable names from the query."""
        # Parse the AST to find all variable references
        from jinja2 import meta

        ast = self._env.parse(self.template_str)
        return meta.find_undeclared_variables(ast)

    @property
    def has_time_variables(self) -> bool:
        """Check if template contains any time variables."""
        return bool(self._variables)

    @property
    def variables(self) -> Set[str]:
        """Return set of variables used in this template."""
        return self._variables.copy()

    def render(self, time_range: TimeRange) -> str:
        """
        Substitute template variables with actual values.

        Args:
            time_range: TimeRange object with start/end times

        Returns:
            Query string with variables substituted

        Raises:
            ValueError: If template uses unsupported variables
        """
        context = {
            "start_time": time_range.start_time,
            "end_time": time_range.end_time,
            "start_time_ms": time_range.start_time_ms,
            "end_time_ms": time_range.end_time_ms,
            "start_datetime": time_range.start_datetime,
            "end_datetime": time_range.end_datetime,
        }

        try:
            return self._template.render(**context)
        except UndefinedError as e:
            unsupported = self._variables - self.SUPPORTED_VARS
            raise ValueError(
                f"Unsupported template variables: {unsupported}. "
                f"Supported: {sorted(self.SUPPORTED_VARS)}"
            ) from e

    @staticmethod
    def calculate_time_range(
        current_time: Optional[int] = None,
        window_seconds: int = 60,
        offset_seconds: int = 0,
    ) -> TimeRange:
        """
        Calculate a time range for query execution.

        The time range is calculated as:
            end_time = current_time - offset_seconds
            start_time = end_time - window_seconds

        Args:
            current_time: Reference Unix timestamp (default: now)
            window_seconds: Size of time window in seconds
            offset_seconds: How far back from current_time to end the window
                           (positive = past, useful for query_time_offset)

        Returns:
            TimeRange object

        Examples:
            # Current time query with 60s window
            calculate_time_range(current_time=1000, window_seconds=60, offset_seconds=0)
            -> TimeRange(start=940, end=1000)

            # Query with 30s offset (for delayed data)
            calculate_time_range(current_time=1000, window_seconds=60, offset_seconds=30)
            -> TimeRange(start=910, end=970)
        """
        if current_time is None:
            current_time = int(time.time())

        end_time = current_time - offset_seconds
        start_time = end_time - window_seconds

        return TimeRange(start_time=start_time, end_time=end_time)
