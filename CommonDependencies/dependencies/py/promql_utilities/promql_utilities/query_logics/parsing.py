from typing import Tuple, List

from promql_utilities.data_model.KeyByLabelNames import KeyByLabelNames
from promql_utilities.query_logics.enums import QueryPatternType, Statistic


def get_metric_and_spatial_filter(query_pattern_match) -> Tuple[str, str]:
    metric = query_pattern_match.tokens["metric"]["name"]
    spatial_filter = ""

    if query_pattern_match.tokens["metric"]["labels"].matchers:
        spatial_filter = (
            query_pattern_match.tokens["metric"]["ast"]
            .prettify()
            .split("{")[1]
            .split("}")[0]
        )
        metric = metric.split("{")[0]

    return metric, spatial_filter


def get_statistics_to_compute(
    query_pattern_type, query_pattern_match
) -> List[Statistic]:
    statistic_to_compute = None

    if (
        query_pattern_type == QueryPatternType.ONLY_TEMPORAL
        or query_pattern_type == QueryPatternType.ONE_TEMPORAL_ONE_SPATIAL
    ):
        statistic_to_compute = query_pattern_match.tokens["function"]["name"].split(
            "_"
        )[0]
        # template_config.tumblingWindowSize = self.t_repeat
    elif query_pattern_type == QueryPatternType.ONLY_SPATIAL:
        statistic_to_compute = query_pattern_match.tokens["aggregation"]["op"]
        # template_config.tumblingWindowSize = self.prometheus_scrape_interval
    else:
        raise ValueError("Invalid query pattern type")

    if statistic_to_compute == "avg":
        return [Statistic.SUM, Statistic.COUNT]
    else:
        # get enum value from string
        return [Statistic[statistic_to_compute.upper()]]


def get_spatial_aggregation_output_labels(
    query_pattern_match, all_labels: KeyByLabelNames
) -> KeyByLabelNames:
    aggregation_modifier = query_pattern_match.tokens["aggregation"]["modifier"]
    aggregation_modifier_labels = None

    # Fixing issue https://github.com/ProjectASAP/asap-internal/issues/24
    if aggregation_modifier is None:
        return KeyByLabelNames([])
    
    if aggregation_modifier.type == aggregation_modifier.type.By:
        aggregation_modifier_labels = KeyByLabelNames(aggregation_modifier.labels)
    elif aggregation_modifier.type == aggregation_modifier.type.Without:
        aggregation_modifier_labels = all_labels - KeyByLabelNames(
            aggregation_modifier.labels
        )
    else:
        raise ValueError("Invalid aggregation modifier")

    return aggregation_modifier_labels
