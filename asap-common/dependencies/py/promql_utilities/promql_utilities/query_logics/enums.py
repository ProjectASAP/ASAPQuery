from enum import Enum, auto


class QueryPatternType(Enum):
    ONLY_TEMPORAL = auto()
    ONLY_SPATIAL = auto()
    ONE_TEMPORAL_ONE_SPATIAL = auto()


class QueryTreatmentType(Enum):
    EXACT = auto()
    APPROXIMATE = auto()


class Statistic(Enum):
    COUNT = auto()
    SUM = auto()
    CARDINALITY = auto()
    INCREASE = auto()
    RATE = auto()
    MIN = auto()
    MAX = auto()
    QUANTILE = auto()
    TOPK = auto()


class QueryResultType(Enum):
    INSTANT_VECTOR = auto()


class CleanupPolicy(Enum):
    """Policy for cleaning up old aggregates from the store."""

    CIRCULAR_BUFFER = "circular_buffer"
    READ_BASED = "read_based"
    NO_CLEANUP = "no_cleanup"
