from typing import Tuple

from promql_utilities.query_logics.enums import QueryTreatmentType, Statistic

# def map_statistic_to_precompute_operators(
#     statistic: str, treatment_type: QueryTreatmentType
# ) -> List[Tuple[str, str]]:
#     # if statistic in ["quantile", "stddev", "stdvar"]:
#     if statistic == "quantile":
#         if treatment_type == QueryTreatmentType.EXACT:
#             raise ValueError(f"Statistic {statistic} cannot be computed exactly")
#         else:
#             return [("KLL", "")]
#         # else:
#         #     return [("UnivMon", "")]
#     elif statistic in ["min", "max"]:
#         if treatment_type == QueryTreatmentType.APPROXIMATE:
#             return [("KLL", "")]
#         else:
#             return [("MinMax", statistic)]
#     elif statistic in ["sum", "count"]:
#         if treatment_type == QueryTreatmentType.APPROXIMATE:
#             return [("CountMinSketch", statistic)]
#         else:
#             return [("Sum", statistic)]
#     elif statistic == "avg":
#         if treatment_type == QueryTreatmentType.APPROXIMATE:
#             return [("CountMinSketch", "sum"), ("CountMinSketch", "count")]
#         else:
#             return [("Sum", "sum"), ("Sum", "count")]
#     elif statistic in ["rate", "increase"]:
#         return [("Increase", "")]
#     else:
#         raise NotImplementedError(f"Statistic {statistic} not supported")


def map_statistic_to_precompute_operator(
    statistic: Statistic, treatment_type: QueryTreatmentType
) -> Tuple[str, str]:
    # if statistic in ["quantile", "stddev", "stdvar"]:
    if statistic == Statistic.QUANTILE:
        if treatment_type == QueryTreatmentType.EXACT:
            raise ValueError(f"Statistic {statistic} cannot be computed exactly")
        else:
            return ("DatasketchesKLL", "")
            # return ("HydraKLL", "")
        # else:
        #     return [("UnivMon", "")]
    elif statistic == Statistic.TOPK:
        if treatment_type == QueryTreatmentType.EXACT:
            raise ValueError(f"Statistic {statistic} cannot be computed exactly")
        else:
            return ("CountMinSketchWithHeap", statistic.name.lower())
    elif statistic in [Statistic.MIN, Statistic.MAX]:
        if treatment_type == QueryTreatmentType.APPROXIMATE:
            return ("DatasketchesKLL", "")
            # return ("HydraKLL", "")
        else:
            # NOTE: Change to Multiple<>Accumulator
            # return ("MinMax", statistic.name.lower())
            return ("MultipleMinMax", statistic.name.lower())
    elif statistic in [Statistic.SUM, Statistic.COUNT]:
        if treatment_type == QueryTreatmentType.APPROXIMATE:
            return ("CountMinSketch", statistic.name.lower())
        else:
            # NOTE: Change to Multiple<>Accumulator
            # return ("Sum", statistic.name.lower())
            return ("MultipleSum", statistic.name.lower())
    # elif statistic == "avg":
    #     if treatment_type == QueryTreatmentType.APPROXIMATE:
    #         return [("CountMinSketch", "sum"), ("CountMinSketch", "count")]
    #     else:
    #         return [("Sum", "sum"), ("Sum", "count")]
    elif statistic in [Statistic.RATE, Statistic.INCREASE]:
        # NOTE: Change to Multiple<>Accumulator
        # return ("Increase", "")
        return ("MultipleIncrease", "")
    else:
        raise NotImplementedError(f"Statistic {statistic} not supported")


def does_precompute_operator_support_subpopulations(
    statistic: Statistic, precompute_operator: str
) -> bool:
    if precompute_operator in ["Increase", "MinMax", "Sum", "DatasketchesKLL"]:
        return False
    elif precompute_operator in [
        "MultipleIncrease",
        "MultipleMinMax",
        "MultipleSum",
        "HydraKLL",
    ]:
        # TODO: do we need to check for statistic here? If not, remove the check from CountMinSketch
        return True
    elif precompute_operator == "CountMinSketch":
        return statistic in [Statistic.SUM, Statistic.COUNT]
    elif (
        precompute_operator == "CountMinSketchWithHeap" and statistic == Statistic.TOPK
    ):
        # topk and bottomk do not support subpopulations!
        # other usages of CountMinSketchWithHeap will fall through.
        return False
    # elif precompute_operator == "UnivMon":
    #     return statistic in ["sum", "count", "avg"]
    else:
        raise NotImplementedError(
            f"Precompute operator {precompute_operator} not supported"
        )


def get_is_collapsable(temporal_aggregation: str, spatial_aggregation: str) -> bool:
    if spatial_aggregation == "sum":
        return temporal_aggregation in [
            "sum_over_time",
            "count_over_time",
            # "increase",
            # "rate",
        ]
    elif spatial_aggregation == "min":
        return temporal_aggregation == "min_over_time"
    elif spatial_aggregation == "max":
        return temporal_aggregation == "max_over_time"
    return False
