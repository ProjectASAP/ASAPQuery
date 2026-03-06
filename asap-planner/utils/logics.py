import copy
import math
from loguru import logger

from promql_utilities.data_model.KeyByLabelNames import KeyByLabelNames
from promql_utilities.query_logics.enums import QueryPatternType, CleanupPolicy
from promql_utilities.ast_matching.PromQLPattern import MatchResult
from promql_utilities.query_logics.logics import (
    does_precompute_operator_support_subpopulations,
)

CMS_WITH_HEAP_MULT = 4

# Default sketch parameters for backward compatibility
DEFAULT_SKETCH_PARAMETERS = {
    "CountMinSketch": {"depth": 3, "width": 1024},
    "CountMinSketchWithHeap": {"depth": 3, "width": 1024, "heap_multiplier": 4},
    "DatasketchesKLL": {"K": 20},
    "HydraKLL": {"row_num": 3, "col_num": 1024, "k": 20},
}


def get_effective_repeat(t_repeat: int, step: int) -> int:
    """
    Calculate effective repeat interval for range queries.

    For range queries (step > 0), use the smaller of t_repeat and step to ensure
    we produce aggregates frequently enough to support the query step size.
    For instant queries (step = 0), use t_repeat.
    """
    return min(t_repeat, step) if step > 0 else t_repeat


# TODO:
# We only show the logic of `get_precompute_operator_parameters` here.
# Semantics for topk query will be added in later PRs.
def get_precompute_operator_parameters(
    aggregation_type: str,
    aggregation_sub_type: str,
    query_pattern_match: MatchResult,
    sketch_parameters: dict,
) -> dict:
    # Allow partial overrides: use provided parameters, fall back to defaults per sketch type
    if sketch_parameters is None:
        sketch_parameters = {}

    if aggregation_type in [
        "Increase",
        "MinMax",
        "Sum",
        "MultipleIncrease",
        "MultipleMinMax",
        "MultipleSum",
        "DeltaSetAggregator",
        "SetAggregator",
    ]:
        return {}
    elif aggregation_type == "CountMinSketch":
        params = sketch_parameters.get(
            "CountMinSketch", DEFAULT_SKETCH_PARAMETERS["CountMinSketch"]
        )
        return {"depth": params["depth"], "width": params["width"]}
    elif aggregation_type == "CountMinSketchWithHeap":
        if aggregation_sub_type == "topk":
            if "aggregation" not in query_pattern_match.tokens:
                raise ValueError(
                    f"{aggregation_sub_type} query missing aggregator in the match tokens"
                )
            if "param" not in query_pattern_match.tokens["aggregation"]:
                raise ValueError(
                    f"{aggregation_sub_type} query missing required 'k' parameter"
                )
            k = int(query_pattern_match.tokens["aggregation"]["param"].val)
            params = sketch_parameters.get(
                "CountMinSketchWithHeap",
                DEFAULT_SKETCH_PARAMETERS["CountMinSketchWithHeap"],
            )
            heap_mult = params.get("heap_multiplier", CMS_WITH_HEAP_MULT)
            return {
                "depth": params["depth"],
                "width": params["width"],
                "heapsize": k * heap_mult,
            }
        else:
            raise ValueError(
                f"Aggregation sub-type {aggregation_sub_type} for CountMinSketchWithHeap not supported"
            )
    elif aggregation_type == "DatasketchesKLL":
        params = sketch_parameters.get(
            "DatasketchesKLL", DEFAULT_SKETCH_PARAMETERS["DatasketchesKLL"]
        )
        return {"K": params["K"]}
    elif aggregation_type == "HydraKLL":
        params = sketch_parameters.get(
            "HydraKLL", DEFAULT_SKETCH_PARAMETERS["HydraKLL"]
        )
        return {
            "row_num": params["row_num"],
            "col_num": params["col_num"],
            "k": params["k"],
        }
    # elif aggregation_type == "UnivMon":
    #     return {"depth": 3, "width": 2048, "levels": 16}
    else:
        raise NotImplementedError(f"Aggregation type {aggregation_type} not supported")


def get_cleanup_param(
    cleanup_policy: CleanupPolicy,
    query_pattern_type,
    query_pattern_match,
    t_repeat: int,
    window_type: str,
    range_duration: int,
    step: int,
) -> int:
    """
    Calculate cleanup parameter based on cleanup policy and range query params.

    Sliding windows (both policies): range_duration / step + 1
    Tumbling circular_buffer: (T_lookback + range_duration) / min(T_repeat, step)
    Tumbling read_based: (T_lookback / min(T_repeat, step)) * (range_duration / step + 1)

    For ONLY_SPATIAL queries, T_lookback = T_repeat.
    For instant queries, range_duration = 0 and effective_repeat = T_repeat.

    Args:
        cleanup_policy: CleanupPolicy.CIRCULAR_BUFFER or CleanupPolicy.READ_BASED
        query_pattern_type: QueryPatternType enum
        query_pattern_match: MatchResult with query tokens
        t_repeat: Query repeat interval in seconds
        window_type: "sliding" or "tumbling"
        range_duration: end - start in seconds (0 for instant queries)
        step: Range query step in seconds (required if range_duration > 0)

    Raises:
        ValueError: If exactly one of range_duration or step is zero
    """
    # Validation: range_duration and step must both be zero (instant) or both non-zero (range)
    if (range_duration == 0) != (step == 0):
        raise ValueError(
            f"range_duration and step must both be 0 (instant query) or both > 0 (range query). "
            f"Got range_duration={range_duration}, step={step}"
        )

    is_range_query = step > 0

    # For ONLY_SPATIAL, T_lookback = T_repeat
    if query_pattern_type == QueryPatternType.ONLY_SPATIAL:
        t_lookback = t_repeat
    else:
        t_lookback = int(
            query_pattern_match.tokens["range_vector"]["range"].total_seconds()
        )

    # For sliding windows: range_duration / step + 1 (same for both policies)
    if window_type == "sliding":
        if is_range_query:
            result = range_duration // step + 1
        else:
            result = 1  # instant query
        logger.debug(
            f"Sliding window mode: cleanup_param = {result} "
            f"(range_duration={range_duration}s, step={step}s)"
        )
        return result

    # Tumbling window calculations
    effective_repeat = get_effective_repeat(t_repeat, step)

    # We use ceiling division because even if the time span doesn't fully fill
    # a bucket, we still need that bucket to cover the partial data.
    # E.g., if T_lookback=10s and effective_repeat=100s, we still need 1 bucket.
    if cleanup_policy == CleanupPolicy.CIRCULAR_BUFFER:
        # ceil((T_lookback + range_duration) / effective_repeat)
        result = math.ceil((t_lookback + range_duration) / effective_repeat)
    elif cleanup_policy == CleanupPolicy.READ_BASED:
        # ceil(T_lookback / effective_repeat) * (range_duration / step + 1)
        lookback_buckets = math.ceil(t_lookback / effective_repeat)
        if is_range_query:
            num_steps = range_duration // step + 1
        else:
            num_steps = 1  # instant query
        result = lookback_buckets * num_steps
    else:
        raise ValueError(f"Invalid cleanup policy: {cleanup_policy}")

    logger.debug(
        f"Tumbling window mode ({cleanup_policy.value}): cleanup_param = {result} "
        f"(t_lookback={t_lookback}s, t_repeat={t_repeat}s, "
        f"range_duration={range_duration}s, step={step}s)"
    )
    return result


def should_use_sliding_window(query_pattern_type, aggregation_type):
    """
    Decide if sliding windows should be used based on query type and aggregation type.

    For Issue #236: Use sliding windows for ALL ONLY_TEMPORAL queries except DeltaSetAggregator.
    This eliminates merging overhead in QueryEngine at the cost of more computation in Arroyo.

    Args:
        query_pattern_type: ONLY_TEMPORAL, ONLY_SPATIAL, or ONE_TEMPORAL_ONE_SPATIAL
        aggregation_type: Type of aggregation (e.g., 'DatasketchesKLL', 'Sum', etc.)

    Returns:
        bool: True if sliding windows should be used
    """
    # NOTE: returning False since sliding window pipelines are causing arroyo to crash
    return False
    # Only use sliding for ONLY_TEMPORAL queries (not ONE_TEMPORAL_ONE_SPATIAL or ONLY_SPATIAL)
    if query_pattern_type != QueryPatternType.ONLY_TEMPORAL:
        logger.debug(
            f"Query pattern {query_pattern_type} not eligible for sliding windows "
            f"(only ONLY_TEMPORAL supported)"
        )
        return False

    # Explicitly exclude DeltaSetAggregator (paired with CMS but needs tumbling)
    if aggregation_type == "DeltaSetAggregator":
        logger.debug("DeltaSetAggregator excluded from sliding windows")
        return False

    # All other ONLY_TEMPORAL aggregations use sliding windows
    logger.info(
        f"Aggregation type '{aggregation_type}' with {query_pattern_type} -> SLIDING windows"
    )
    return True


def set_window_parameters(
    query_pattern_type,
    query_pattern_match,
    t_repeat,
    prometheus_scrape_interval,
    aggregation_type,
    template_config,
    step: int,
):
    """
    Set window parameters for streaming aggregation config.
    Auto-decides between sliding and tumbling windows based on query type and aggregation cost.

    For ONLY_TEMPORAL queries with expensive aggregations (KLL, CMS):
    - Uses SLIDING windows: windowSize = range duration, slideInterval = effective_repeat
    - This reduces QueryEngine latency by avoiding merges (Arroyo does more work upfront)

    For other queries:
    - Uses TUMBLING windows: windowSize = slideInterval = effective_repeat
    - This is the original behavior

    For range queries (step > 0), effective_repeat = min(t_repeat, step).
    For instant queries (step = 0), effective_repeat = t_repeat.

    Args:
        query_pattern_type: Pattern type (ONLY_TEMPORAL, ONLY_SPATIAL, ONE_TEMPORAL_ONE_SPATIAL)
        query_pattern_match: Matched PromQL pattern containing query metadata
        t_repeat: Query repeat interval in seconds
        prometheus_scrape_interval: Scrape interval in seconds
        aggregation_type: Type of aggregation operator
        template_config: StreamingAggregationConfig to update
        step: Range query step in seconds (0 for instant queries)
    """
    # For range queries, use min(t_repeat, step) as the effective repeat interval
    effective_repeat = get_effective_repeat(t_repeat, step)

    # Decide if we should use sliding windows
    use_sliding_window = should_use_sliding_window(query_pattern_type, aggregation_type)

    if use_sliding_window:
        # SLIDING WINDOW for ONLY_TEMPORAL queries with expensive aggregations
        logger.info(
            f"Configuring SLIDING WINDOW for {query_pattern_type} "
            f"with {aggregation_type}"
        )

        if query_pattern_type == QueryPatternType.ONLY_TEMPORAL:
            # Window size = range duration (e.g., 15m = 900s)
            range_seconds = int(
                query_pattern_match.tokens["range_vector"]["range"].total_seconds()
            )

            # Check if this is actually a tumbling window (windowSize == slideInterval)
            if range_seconds == effective_repeat:
                logger.info(
                    f"Detected windowSize == slideInterval ({range_seconds}s). "
                    f"Using tumbling window instead of sliding for efficiency."
                )
                template_config.windowSize = effective_repeat
                template_config.slideInterval = effective_repeat
                template_config.windowType = "tumbling"
                template_config.tumblingWindowSize = effective_repeat
            else:
                # True sliding window
                template_config.windowSize = range_seconds
                template_config.slideInterval = effective_repeat
                template_config.windowType = "sliding"

                logger.info(
                    f"Sliding window params: windowSize={range_seconds}s, "
                    f"slideInterval={effective_repeat}s "
                    f"(each window has {range_seconds} seconds of data, slides every {effective_repeat}s)"
                )

                # Set deprecated field for backward compatibility
                template_config.tumblingWindowSize = effective_repeat
        else:
            # This should never be reached due to should_use_sliding_window() check
            assert False, (
                f"should_use_sliding_window returned True for {query_pattern_type}, "
                f"but sliding windows only supported for ONLY_TEMPORAL"
            )
    else:
        # TUMBLING WINDOW (existing logic)
        logger.info(
            f"Configuring TUMBLING WINDOW for {query_pattern_type} "
            f"with {aggregation_type}"
        )
        _set_tumbling_window_parameters(
            query_pattern_type,
            effective_repeat,
            prometheus_scrape_interval,
            template_config,
        )


def _set_tumbling_window_parameters(
    query_pattern_type, effective_repeat, prometheus_scrape_interval, template_config
):
    """
    Set tumbling window parameters.

    Args:
        query_pattern_type: Pattern type (ONLY_TEMPORAL, ONLY_SPATIAL, ONE_TEMPORAL_ONE_SPATIAL)
        effective_repeat: Effective repeat interval (min(t_repeat, step) for range queries)
        prometheus_scrape_interval: Scrape interval in seconds
        template_config: StreamingAggregationConfig to update
    """
    if (
        query_pattern_type == QueryPatternType.ONLY_TEMPORAL
        or query_pattern_type == QueryPatternType.ONE_TEMPORAL_ONE_SPATIAL
    ):
        template_config.windowSize = effective_repeat
        template_config.slideInterval = effective_repeat
        template_config.windowType = "tumbling"
        template_config.tumblingWindowSize = effective_repeat

        logger.debug(
            f"Tumbling window params: windowSize={effective_repeat}s, slideInterval={effective_repeat}s"
        )
    elif query_pattern_type == QueryPatternType.ONLY_SPATIAL:
        template_config.windowSize = prometheus_scrape_interval
        template_config.slideInterval = prometheus_scrape_interval
        template_config.windowType = "tumbling"
        template_config.tumblingWindowSize = prometheus_scrape_interval

        logger.debug(
            f"Tumbling window params: windowSize={prometheus_scrape_interval}s, "
            f"slideInterval={prometheus_scrape_interval}s"
        )
    else:
        raise ValueError("Invalid query pattern type")


# COMMENTED OUT - Original function kept for rollback
# Issue #236: Replaced with set_window_parameters() to support sliding windows
#
# def set_tumbling_window_size(
#     query_pattern_type, t_repeat, prometheus_scrape_interval, template_config
# ):
#     if (
#         query_pattern_type == QueryPatternType.ONLY_TEMPORAL
#         or query_pattern_type == QueryPatternType.ONE_TEMPORAL_ONE_SPATIAL
#     ):
#         template_config.tumblingWindowSize = t_repeat
#     elif query_pattern_type == QueryPatternType.ONLY_SPATIAL:
#         template_config.tumblingWindowSize = prometheus_scrape_interval
#     else:
#         raise ValueError("Invalid query pattern type")


def set_subpopulation_labels(
    statistic_to_compute,
    aggregation_type,
    subpopulation_labels: KeyByLabelNames,
    template_config,
):
    if does_precompute_operator_support_subpopulations(
        statistic_to_compute, aggregation_type
    ):
        template_config.labels["grouping"] = KeyByLabelNames([])
        template_config.labels["aggregated"] = copy.deepcopy(subpopulation_labels)
    else:
        template_config.labels["grouping"] = copy.deepcopy(subpopulation_labels)
        template_config.labels["aggregated"] = KeyByLabelNames([])
