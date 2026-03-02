import os
import yaml
import argparse
from loguru import logger

from classes.SingleQueryConfig import SingleQueryConfig
from promql_utilities.streaming_config.MetricConfig import MetricConfig
from promql_utilities.query_logics.enums import CleanupPolicy


def read_config(config_path) -> dict:
    config_yaml = None
    with open(config_path, "r") as f:
        config_yaml = yaml.safe_load(f)
    return config_yaml


def validate_config(config_yaml):
    # NOTE: only allow unique query strings for now
    query_strings = set()
    for query_group_yaml in config_yaml["query_groups"]:
        for query_string in query_group_yaml["queries"]:
            if query_string in query_strings:
                raise ValueError(f"Duplicate query string: {query_string}")
            query_strings.add(query_string)


def main(args):
    input_config_yaml = read_config(args.input_config)

    validate_config(input_config_yaml)

    metric_config = MetricConfig.from_list(input_config_yaml["metrics"])

    # Read cleanup policy configuration (default to READ_BASED if not specified)
    cleanup_policy_str = input_config_yaml.get("aggregate_cleanup", {}).get(
        "policy", "read_based"
    )
    try:
        cleanup_policy = CleanupPolicy(cleanup_policy_str)
    except ValueError:
        valid_policies = [p.value for p in CleanupPolicy]
        raise ValueError(
            f"Invalid cleanup policy: '{cleanup_policy_str}'. "
            f"Valid options: {valid_policies}"
        )
    logger.info("Cleanup policy: {}", cleanup_policy.value)

    # Read sketch parameters configuration (use None to apply defaults in logics.py)
    sketch_parameters = input_config_yaml.get("sketch_parameters", None)
    if sketch_parameters:
        logger.info("Using custom sketch parameters: {}", sketch_parameters)
    else:
        logger.info("Using default sketch parameters")

    streaming_aggregation_configs_map = {}
    query_aggregation_config_keys_map = {}

    for query_group_yaml in input_config_yaml["query_groups"]:
        for query_string in query_group_yaml["queries"]:
            single_query_config_yaml = {
                "query": query_string,
                "t_repeat": query_group_yaml["repetition_delay"],
                "options": query_group_yaml["controller_options"],
                "cleanup_policy": cleanup_policy,
                "range_duration": args.range_duration,
                "step": args.step,
            }

            logger.debug("Processing query {}", query_string)

            single_query_config = SingleQueryConfig(
                single_query_config_yaml,
                metric_config,
                args.prometheus_scrape_interval,
                args.streaming_engine,
                sketch_parameters,
            )

            should_process_query = single_query_config.is_supported()
            if args.enable_punting:
                should_process_query = (
                    should_process_query and single_query_config.should_be_performant()
                )

            if should_process_query:
                query_aggregation_config_keys_map[single_query_config.query] = []
                current_configs, num_aggregates_to_retain = (
                    single_query_config.get_streaming_aggregation_configs()
                )

                for current_config in current_configs:
                    key = current_config.get_identifying_key()
                    query_aggregation_config_keys_map[single_query_config.query].append(
                        (key, num_aggregates_to_retain)
                    )
                    if key not in streaming_aggregation_configs_map:
                        streaming_aggregation_configs_map[key] = current_config
            else:
                logger.warning("Unsupported query")

    for idx, k in enumerate(streaming_aggregation_configs_map.keys()):
        streaming_aggregation_configs_map[k].aggregationId = idx + 1

    streaming_config = {
        "aggregations": [
            config.to_dict(metric_config, "promql")
            for config in streaming_aggregation_configs_map.values()
        ],
        "metrics": metric_config.config,
    }
    inference_config = {
        "cleanup_policy": {"name": cleanup_policy.value},
        "queries": [],
        "metrics": metric_config.config,
    }
    for query, streaming_config_keys in query_aggregation_config_keys_map.items():
        inference_config["queries"].append({"query": query, "aggregations": []})
        for streaming_config_key in streaming_config_keys:
            aggregation_entry = {
                "aggregation_id": streaming_aggregation_configs_map[
                    streaming_config_key[0]
                ].aggregationId,
            }
            # Add the appropriate parameter based on cleanup policy
            cleanup_value = streaming_config_key[1]
            if (
                cleanup_policy == CleanupPolicy.CIRCULAR_BUFFER
                and cleanup_value is not None
            ):
                aggregation_entry["num_aggregates_to_retain"] = cleanup_value
            elif (
                cleanup_policy == CleanupPolicy.READ_BASED and cleanup_value is not None
            ):
                aggregation_entry["read_count_threshold"] = cleanup_value
            # For NO_CLEANUP, we don't add any parameter
            inference_config["queries"][-1]["aggregations"].append(aggregation_entry)

    os.makedirs(args.output_dir, exist_ok=True)
    with open(f"{args.output_dir}/streaming_config.yaml", "w") as f:
        f.write(yaml.dump(streaming_config))

    with open(f"{args.output_dir}/inference_config.yaml", "w") as f:
        f.write(yaml.dump(inference_config))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_config", type=str, required=True)
    parser.add_argument("--output_dir", type=str, required=True)
    parser.add_argument("--prometheus_scrape_interval", type=int, required=True)
    parser.add_argument(
        "--streaming_engine", type=str, choices=["flink", "arroyo"], required=True
    )
    parser.add_argument(
        "--enable-punting",
        action="store_true",
        help="Enable query punting based on performance heuristics",
    )
    parser.add_argument(
        "--range-duration",
        type=int,
        default=0,
        help="Range query duration (end - start) in seconds. 0 for instant queries.",
    )
    parser.add_argument(
        "--step",
        type=int,
        default=0,
        help="Range query step in seconds. Required if range-duration > 0.",
    )
    args = parser.parse_args()
    main(args)
