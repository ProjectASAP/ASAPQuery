import copy
from loguru import logger

import promql_parser
from typing import Optional, Tuple, List

from promql_utilities.ast_matching.PromQLPattern import PromQLPattern, MatchResult
from promql_utilities.ast_matching.PromQLPatternBuilder import PromQLPatternBuilder
from promql_utilities.query_logics.enums import (
    QueryPatternType,
    QueryTreatmentType,
    CleanupPolicy,
)
from promql_utilities.query_logics.logics import (
    get_is_collapsable,
    map_statistic_to_precompute_operator,
)
from promql_utilities.query_logics.parsing import (
    get_metric_and_spatial_filter,
    get_statistics_to_compute,
)
from promql_utilities.query_logics.parsing import get_spatial_aggregation_output_labels
from promql_utilities.data_model.KeyByLabelNames import KeyByLabelNames

from promql_utilities.streaming_config.StreamingAggregationConfig import (
    StreamingAggregationConfig,
)
from utils import logics

# import utils.promql

from promql_utilities.streaming_config.MetricConfig import MetricConfig


class SingleQueryConfig:
    def __init__(
        self,
        config: dict,
        metric_config: MetricConfig,
        prometheus_scrape_interval: int,
        streaming_engine: str,
        sketch_parameters: dict,
    ):
        self.config = config
        self.query = config["query"]
        self.query_ast = promql_parser.parse(self.query)
        self.t_repeat = int(config["t_repeat"])
        self.prometheus_scrape_interval = prometheus_scrape_interval
        self.__dict__.update(config["options"])
        # self.accuracy_sla = float(config["accuracy_sla"])
        # self.latency_sla = float(config["latency_sla"])
        self.metric_config = metric_config
        self.streaming_engine = streaming_engine
        self.sketch_parameters = sketch_parameters
        self.range_duration = config["range_duration"]
        self.step = config["step"]

        self.patterns = {
            QueryPatternType.ONLY_TEMPORAL: [
                PromQLPattern(
                    PromQLPatternBuilder.function(
                        "quantile_over_time",
                        PromQLPatternBuilder.number(),
                        PromQLPatternBuilder.matrix_selector(
                            PromQLPatternBuilder.metric(collect_as="metric"),
                            collect_as="range_vector",
                        ),
                        collect_as="function",
                        collect_args_as="function_args",
                    )
                ),
                PromQLPattern(
                    PromQLPatternBuilder.function(
                        [
                            "sum_over_time",
                            "count_over_time",
                            "avg_over_time",
                            "min_over_time",
                            "max_over_time",
                            # "stddev_over_time",
                            # "stdvar_over_time",
                            "increase",
                            "rate",
                        ],
                        PromQLPatternBuilder.matrix_selector(
                            PromQLPatternBuilder.metric(collect_as="metric"),
                            collect_as="range_vector",
                        ),
                        collect_as="function",
                        collect_args_as="function_args",
                    )
                ),
            ],
            # TODO: add topk/bottomk
            QueryPatternType.ONLY_SPATIAL: [
                PromQLPattern(
                    PromQLPatternBuilder.aggregation(
                        [
                            "sum",
                            "count",
                            "avg",
                            "quantile",
                            "min",
                            "max",
                            "topk",
                            # "stddev",
                            # "stdvar",
                        ],
                        PromQLPatternBuilder.metric(collect_as="metric"),
                        collect_as="aggregation",
                    )
                )
            ],
            # TODO: need some way of specifying pattern using an existing pattern
            QueryPatternType.ONE_TEMPORAL_ONE_SPATIAL: [
                PromQLPattern(
                    PromQLPatternBuilder.aggregation(
                        [
                            "sum",
                            "count",
                            "avg",
                            "quantile",
                            "min",
                            "max",
                            # "stddev",
                            # "stdvar",
                        ],
                        PromQLPatternBuilder.function(
                            "quantile_over_time",
                            PromQLPatternBuilder.number(),
                            PromQLPatternBuilder.matrix_selector(
                                PromQLPatternBuilder.metric(collect_as="metric"),
                                collect_as="range_vector",
                            ),
                            collect_as="function",
                            collect_args_as="function_args",
                        ),
                        collect_as="aggregation",
                    )
                ),
                PromQLPattern(
                    PromQLPatternBuilder.aggregation(
                        [
                            "sum",
                            "count",
                            "avg",
                            "quantile",
                            "min",
                            "max",
                            # "stddev",
                            # "stdvar",
                        ],
                        PromQLPatternBuilder.function(
                            [
                                "sum_over_time",
                                "count_over_time",
                                "avg_over_time",
                                "min_over_time",
                                "max_over_time",
                                # "stddev_over_time",
                                # "stdvar_over_time",
                                "increase",
                                "rate",
                            ],
                            PromQLPatternBuilder.matrix_selector(
                                PromQLPatternBuilder.metric(collect_as="metric"),
                                collect_as="range_vector",
                            ),
                            collect_as="function",
                            collect_args_as="function_args",
                        ),
                        collect_as="aggregation",
                    )
                ),
            ],
        }

        self.query_pattern_type = None
        self.query_pattern_match = None
        self.query_treatment_type = None

        self.process_query()

    def process_query(self):
        query_pattern_type, match = self.match_query_pattern()

        if query_pattern_type and match:
            self.query_pattern_type = query_pattern_type
            self.query_pattern_match = match
            self.query_treatment_type = self.get_query_treatment_type()
            logger.debug("Query treatment type: {}", self.query_treatment_type)
        else:
            # self.logger.warning("Query pattern not supported: %s", self.query)
            logger.warning("Query pattern not supported: {}", self.query)

    def should_be_performant(self) -> bool:
        if self.query_pattern_type == QueryPatternType.ONLY_TEMPORAL:
            # Check quantile_over_time, rate, increase
            # Calculate number of data points per key
            function_name = self.query_pattern_match.tokens["function"]["name"]
            if function_name in ["rate", "increase", "quantile_over_time"]:
                num_data_points_per_tumbling_window = (
                    self.t_repeat / self.prometheus_scrape_interval
                )
                range_duration = int(
                    self.query_pattern_match.tokens["range_vector"][
                        "range"
                    ].total_seconds()
                )
                if num_data_points_per_tumbling_window < 60:
                    logger.info(
                        "[Performance Check Failed] num_data_points_per_tumbling_window {} < 60",
                        num_data_points_per_tumbling_window,
                    )
                    return False
                # bound time for merging for quantile_over_time
                if function_name == "quantile_over_time":
                    if range_duration / self.t_repeat > 15:
                        logger.info(
                            "[Performance Check Failed] range_duration / t_repeat {} > 15",
                            range_duration / self.t_repeat,
                        )
                        return False
            return True
        elif self.query_pattern_type == QueryPatternType.ONLY_SPATIAL:
            return True
        elif self.query_pattern_type == QueryPatternType.ONE_TEMPORAL_ONE_SPATIAL:
            # TODO: might need to add checks here
            return True
        else:
            return True

    def is_supported(self) -> bool:
        return (
            self.query_pattern_type is not None and self.query_pattern_match is not None
        )

    def match_query_pattern(
        self,
    ) -> Tuple[Optional[QueryPatternType], Optional[MatchResult]]:
        for pattern_type, patterns in self.patterns.items():
            for pattern in patterns:
                match = pattern.matches(self.query_ast, debug=False)
                if match:
                    logger.debug("Matched pattern: {}", pattern_type)
                    return pattern_type, match
        return None, None

    def get_query_treatment_type(self):
        assert self.query_pattern_type and self.query_pattern_match

        if (
            self.query_pattern_type == QueryPatternType.ONLY_TEMPORAL
            or self.query_pattern_type == QueryPatternType.ONE_TEMPORAL_ONE_SPATIAL
        ):
            if self.query_pattern_match.tokens["function"]["name"] in [
                "quantile_over_time",
                "sum_over_time",
                "count_over_time",
                "avg_over_time",
            ]:
                return QueryTreatmentType.APPROXIMATE
            else:
                return QueryTreatmentType.EXACT
        elif self.query_pattern_type == QueryPatternType.ONLY_SPATIAL:
            if self.query_pattern_match.tokens["aggregation"]["op"] in [
                "quantile",
                "sum",
                "count",
                "avg",
                "topk",
            ]:
                return QueryTreatmentType.APPROXIMATE
            else:
                return QueryTreatmentType.EXACT
        else:
            raise ValueError("Invalid query pattern type")

    def get_streaming_aggregation_configs(
        self,
    ) -> Tuple[List[StreamingAggregationConfig], int]:
        assert (
            self.query_pattern_type
            and self.query_pattern_match
            and self.query_treatment_type
        )

        template_config = StreamingAggregationConfig()
        template_config.aggregationId = -1
        # template_config.metric = self.query_pattern_match.tokens["metric"]["name"]

        # setting spatial filter
        # if self.query_pattern_match.tokens["metric"]["labels"].matchers:
        #     template_config.spatialFilter = (
        #         self.query_pattern_match.tokens["metric"]["ast"]
        #         .prettify()
        #         .split("{")[1]
        #         .split("}")[0]
        #     )
        #     template_config.metric = template_config.metric.split("{")[0]
        # else:
        #     template_config.spatialFilter = ""

        template_config.metric, template_config.spatialFilter = (
            get_metric_and_spatial_filter(self.query_pattern_match)
        )

        statistics_to_compute = get_statistics_to_compute(
            self.query_pattern_type, self.query_pattern_match
        )

        # if (
        #     self.query_pattern_type == QueryPatternType.ONLY_TEMPORAL
        #     or self.query_pattern_type == QueryPatternType.ONE_TEMPORAL_ONE_SPATIAL
        # ):
        #     statistic_to_compute = self.query_pattern_match.tokens["function"][
        #         "name"
        #     ].split("_")[0]
        #     template_config.tumblingWindowSize = self.t_repeat
        # elif self.query_pattern_type == QueryPatternType.ONLY_SPATIAL:
        #     statistic_to_compute = self.query_pattern_match.tokens["aggregation"]["op"]
        #     template_config.tumblingWindowSize = self.prometheus_scrape_interval
        # else:
        #     raise ValueError("Invalid query pattern type")

        configs = []

        for statistic_to_compute in statistics_to_compute:

            aggregation_type, aggregation_sub_type = (
                map_statistic_to_precompute_operator(
                    statistic_to_compute, self.query_treatment_type
                )
            )

            # NEW: Set window parameters (auto-decides sliding vs tumbling based on query type)
            # Issue #236: Sliding windows for ONLY_TEMPORAL queries (except DeltaSetAggregator)
            # Issue #329: For range queries, use min(t_repeat, step) as effective repeat interval
            logics.set_window_parameters(
                self.query_pattern_type,
                self.query_pattern_match,
                self.t_repeat,
                self.prometheus_scrape_interval,
                aggregation_type,
                template_config,
                self.step,
            )

            # for aggregation_type, aggregation_sub_type in list_of_precompute_operators:

            all_labels = self.metric_config.config[template_config.metric]

            if self.query_pattern_type == QueryPatternType.ONLY_TEMPORAL:
                template_config.labels["rollup"] = KeyByLabelNames([])

                logics.set_subpopulation_labels(
                    statistic_to_compute, aggregation_type, all_labels, template_config
                )

                # if logics.does_precompute_operator_support_subpopulations(
                #     statistic_to_compute, aggregation_type
                # ):
                #     template_config.labels["grouping"] = KeyByLabelNames([])
                #     template_config.labels["aggregated"] = copy.deepcopy(
                #         self.metric_config.config[template_config.metric]
                #     )
                # else:
                #     template_config.labels["grouping"] = copy.deepcopy(
                #         self.metric_config.config[template_config.metric]
                #     )
                #     template_config.labels["aggregated"] = KeyByLabelNames([])

            elif self.query_pattern_type == QueryPatternType.ONLY_SPATIAL:
                # aggregation_modifier = self.query_pattern_match.tokens["aggregation"][
                #     "modifier"
                # ]
                # aggregation_modifier_labels = None
                # if aggregation_modifier.type == aggregation_modifier.type.By:
                #     aggregation_modifier_labels = KeyByLabelNames(
                #         aggregation_modifier.labels
                #     )
                # elif aggregation_modifier.type == aggregation_modifier.type.Without:
                #     aggregation_modifier_labels = self.metric_config.config[
                #         template_config.metric
                #     ] - KeyByLabelNames(aggregation_modifier.labels)
                # else:
                #     raise ValueError("Invalid aggregation modifier")

                spatial_aggregation_output_labels = (
                    get_spatial_aggregation_output_labels(
                        self.query_pattern_match, all_labels
                    )
                )

                template_config.labels["rollup"] = (
                    all_labels - spatial_aggregation_output_labels
                )

                logics.set_subpopulation_labels(
                    statistic_to_compute,
                    aggregation_type,
                    spatial_aggregation_output_labels,
                    template_config,
                )

                # if logics.does_precompute_operator_support_subpopulations(
                #     statistic_to_compute, aggregation_type
                # ):
                #     template_config.labels["aggregated"] = copy.deepcopy(
                #         aggregation_modifier_labels
                #     )
                #     template_config.labels["grouping"] = KeyByLabelNames([])
                # else:
                #     template_config.labels["aggregated"] = KeyByLabelNames([])
                #     template_config.labels["grouping"] = copy.deepcopy(
                #         aggregation_modifier_labels
                #     )

            elif self.query_pattern_type == QueryPatternType.ONE_TEMPORAL_ONE_SPATIAL:
                collapsable = get_is_collapsable(
                    self.query_pattern_match.tokens["function"]["name"],
                    self.query_pattern_match.tokens["aggregation"]["op"],
                )

                if not collapsable:
                    template_config.labels["rollup"] = KeyByLabelNames([])

                    logics.set_subpopulation_labels(
                        statistic_to_compute,
                        aggregation_type,
                        all_labels,
                        template_config,
                    )

                    # if logics.does_precompute_operator_support_subpopulations(
                    #     statistic_to_compute, aggregation_type
                    # ):
                    #     template_config.labels["grouping"] = KeyByLabelNames([])
                    #     template_config.labels["aggregated"] = copy.deepcopy(
                    #         self.metric_config.config[template_config.metric]
                    #     )
                    # else:
                    #     template_config.labels["grouping"] = copy.deepcopy(
                    #         self.metric_config.config[template_config.metric]
                    #     )
                    #     template_config.labels["aggregated"] = KeyByLabelNames([])
                else:
                    # aggregation_modifier = self.query_pattern_match.tokens[
                    #     "aggregation"
                    # ]["modifier"]
                    # aggregation_modifier_labels = None
                    # if aggregation_modifier.type == aggregation_modifier.type.By:
                    #     aggregation_modifier_labels = KeyByLabelNames(
                    #         aggregation_modifier.labels
                    #     )
                    # elif aggregation_modifier.type == aggregation_modifier.type.Without:
                    #     aggregation_modifier_labels = self.metric_config.config[
                    #         template_config.metric
                    #     ] - KeyByLabelNames(aggregation_modifier.labels)
                    # else:
                    #     raise ValueError("Invalid aggregation modifier")

                    spatial_aggregation_output_labels = (
                        get_spatial_aggregation_output_labels(
                            self.query_pattern_match, all_labels
                        )
                    )

                    template_config.labels["rollup"] = (
                        all_labels - spatial_aggregation_output_labels
                    )

                    logics.set_subpopulation_labels(
                        statistic_to_compute,
                        aggregation_type,
                        spatial_aggregation_output_labels,
                        template_config,
                    )

                    # if logics.does_precompute_operator_support_subpopulations(
                    #     statistic_to_compute, aggregation_type
                    # ):
                    #     template_config.labels["aggregated"] = copy.deepcopy(
                    #         aggregation_modifier_labels
                    #     )
                    #     template_config.labels["grouping"] = KeyByLabelNames([])
                    # else:
                    #     template_config.labels["aggregated"] = KeyByLabelNames([])
                    #     template_config.labels["grouping"] = copy.deepcopy(
                    #         aggregation_modifier_labels
                    #     )

            config = copy.deepcopy(template_config)
            config.aggregationType = aggregation_type
            config.aggregationSubType = aggregation_sub_type
            config.parameters = logics.get_precompute_operator_parameters(
                aggregation_type,
                aggregation_sub_type,
                self.query_pattern_match,
                self.sketch_parameters,
            )

            # TODO: remove this hardcoding once promql_utilities.query_logics has updated logic
            # https://github.com/SketchDB/Utilities/issues/44
            if aggregation_type in ["CountMinSketch", "HydraKLL"]:
                # add another precompute operator for DeltaSetAggregator
                delta_set_config = copy.deepcopy(template_config)
                if (
                    self.streaming_engine == "flink"
                    or self.streaming_engine == "arroyo"
                ):
                    delta_set_config.aggregationType = "DeltaSetAggregator"
                else:
                    raise ValueError(
                        f"Unsupported streaming engine: {self.streaming_engine}"
                    )
                delta_set_config.aggregationSubType = ""
                delta_set_config.parameters = logics.get_precompute_operator_parameters(
                    delta_set_config.aggregationType,
                    delta_set_config.aggregationSubType,
                    self.query_pattern_match,
                    self.sketch_parameters,
                )
                configs.append(delta_set_config)
            configs.append(config)

        # Calculate cleanup parameter based on cleanup policy and window type
        # This must be done AFTER set_window_parameters() has been called
        cleanup_policy = self.config["cleanup_policy"]
        if cleanup_policy == CleanupPolicy.NO_CLEANUP:
            logger.info("Cleanup policy is NO_CLEANUP - cleanup_param will be None")
            cleanup_param = None
        else:
            cleanup_param = logics.get_cleanup_param(
                cleanup_policy=cleanup_policy,
                query_pattern_type=self.query_pattern_type,
                query_pattern_match=self.query_pattern_match,
                t_repeat=self.t_repeat,
                window_type=template_config.windowType,
                range_duration=self.range_duration,
                step=self.step,
            )

        return configs, cleanup_param
