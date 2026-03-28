import os
import json
import yaml
import argparse
from loguru import logger
from jinja2 import Template
from typing import Tuple, List

from utils import arroyo_utils, http_utils, jinja_utils
from promql_utilities.streaming_config.MetricConfig import MetricConfig
from promql_utilities.streaming_config.SQLTableConfig import SQLTableConfig, TableSchema
from promql_utilities.streaming_config.StreamingAggregationConfig import (
    StreamingAggregationConfig,
)


def check_args(args):
    if args.output_file_path:
        raise NotImplementedError("Output file path is not implemented yet")

    # Validate source type specific parameters
    if args.source_type == "kafka":
        if args.input_kafka_topic is None:
            raise ValueError("Input Kafka topic is required when using Kafka source")
        if args.kafka_input_format != "json":
            raise NotImplementedError(
                "Kafka input format {} is not implemented yet".format(
                    args.kafka_input_format
                )
            )
    elif args.source_type == "prometheus_remote_write":
        if args.prometheus_base_port is None:
            raise ValueError(
                "Prometheus base port is required when using prometheus_remote_write source"
            )
        if args.prometheus_path is None:
            raise ValueError(
                "Prometheus path is required when using prometheus_remote_write source"
            )
        if args.prometheus_bind_ip is None:
            raise ValueError(
                "Prometheus bind IP is required when using prometheus_remote_write source"
            )
    elif args.source_type == "file":
        if args.input_file_path is None:
            raise ValueError("Input file path is required when using file source")
        if args.file_format is None:
            raise ValueError("--file_format is required when using file source")
        if args.ts_format is None:
            raise ValueError("--ts_format is required when using file source")
        if args.query_language != "sql":
            raise ValueError(
                "File source only supports --query_language sql, got: {}".format(
                    args.query_language
                )
            )

    if args.output_kafka_topic is None:
        raise ValueError("Output Kafka topic is required")

    if args.output_format != "json":
        raise NotImplementedError(
            "Output format {} is not implemented yet".format(args.output_format)
        )


def create_connection_profile(args, template_dir) -> str:
    """Create a connection profile JSON based on template"""
    template = jinja_utils.load_template(template_dir, "connection_profile.j2")

    rendered = template.render(
        profile_name=args.profile_name, bootstrap_servers=args.bootstrap_servers
    )

    # Save to file
    output_path = os.path.join(args.output_dir, "connection_profile.json")
    with open(output_path, "w") as f:
        f.write(rendered)

    print(f"Created connection profile at: {output_path}")

    if args.dry_run:
        # Generate a dummy profile ID for dry run
        profile_id = "dry_run_profile_id"
        print(f"[DRY RUN] Would create connection profile with ID: {profile_id}")
        return profile_id

    # If API URL provided, create connection profile via API
    response = http_utils.create_arroyo_resource(
        arroyo_url=args.arroyo_url,
        endpoint="connection_profiles",
        data=rendered,
        resource_type="connection profile",
    )
    profile_id = json.loads(response).get("id")

    return profile_id


def delete_connection_profile(args):
    if args.dry_run:
        print(
            f"[DRY RUN] Would delete connection profiles with name: {args.profile_name}"
        )
        return

    # list all connection profiles
    response = http_utils.make_api_request(
        url=f"{args.arroyo_url}/connection_profiles",
        method="get",
    )
    response = json.loads(response)

    # get the ID of the connection profile with the name args.profile_name
    profiles = [
        profile for profile in response["data"] if profile["name"] == args.profile_name
    ]
    if len(profiles) == 0:
        print(f"No connection profile found with name {args.profile_name}")
        return

    # delete the connection profile with the ID
    for profile in profiles:
        http_utils.make_api_request(
            url=f"{args.arroyo_url}/connection_profiles/{profile['id']}",
            method="delete",
        )


def create_source_connection_table(
    args,
    topic_name,
    table_name,
    profile_id,
    metric_labels: List[str],
    template_dir,
    query_language: str,
    metrics_dict=None,
    table_schema: TableSchema = None,
):
    """Create a connection table JSON (source) based on template

    Args:
        metrics_dict: For optimized source only. Dictionary mapping metric names to their label lists.
                     e.g., {"cpu_usage": ["instance", "job"], "memory_usage": ["instance", "node"]}
        query_language: "promql" or "sql" - determines schema structure
        table_schema: For SQL mode, the TableSchema for this table
    """

    # Select template based on source type and query language
    if args.source_type == "kafka":
        if query_language == "sql":
            template_name = "connection_table_kafka_sql.j2"
        else:
            template_name = "connection_table_kafka.j2"
    elif args.source_type == "prometheus_remote_write":
        if args.prometheus_remote_write_source == "optimized":
            template_name = "connection_table_prometheus_remote_write_optimized.j2"
        else:
            template_name = "connection_table_prometheus_remote_write.j2"
    elif args.source_type == "file":
        template_name = "connection_table_file.j2"
    else:
        raise ValueError(f"Unsupported source type: {args.source_type}")

    template = jinja_utils.load_template(template_dir, template_name)

    # Create JSON schema definition for label fields
    label_properties = {}
    label_fields_json = []

    for field in metric_labels:
        # Add field to JSON schema properties
        label_properties[field] = {"type": "string", "description": f"{field} label"}

        # Add field to fields array for schema
        label_fields_json.append(
            {
                "fieldName": field,
                "fieldType": {"type": {"primitive": "String"}, "sqlName": "TEXT"},
                "nullable": False,
                "metadataKey": None,
            }
        )

    # Generate the complete JSON schema definition
    json_schema = {
        "type": "object",
        "required": ["labels", "value", "name", "timestamp"],
        "properties": {
            "labels": {
                "type": "object",
                "required": metric_labels,
                "properties": label_properties,
                "additionalProperties": False,
            },
            "value": {"type": "number", "description": "Metric value"},
            "name": {"type": "string", "description": "Metric name"},
            "timestamp": {
                "type": "string",
                "format": "date-time",
                "description": "Time when the metric was recorded, in RFC 3339 format",
            },
        },
        "additionalProperties": False,
    }

    if args.source_type == "kafka":
        json_schema["properties"]["timestamp"] = {
            "type": "string",
            "format": "date-time",
            "description": "Time when the metric was recorded, in RFC 3339 format",
        }
    elif args.source_type == "prometheus_remote_write":
        json_schema["properties"]["timestamp"] = {
            "type": "integer",
            "description": "Unix timestamp in milliseconds when the metric was recorded",
        }

    template_vars = {
        "table_name": table_name,
        "label_fields": label_fields_json,
        "json_schema": json.dumps(json_schema, indent=2)
        .replace("\n", "\\n")
        .replace('"', '\\"'),
    }

    if args.source_type == "kafka":
        template_vars["topic_name"] = topic_name
        template_vars["profile_id"] = profile_id

        # For SQL mode, override template_vars with flat schema
        if query_language == "sql" and table_schema is not None:
            sql_json_schema = build_sql_json_schema(table_schema)
            template_vars = {
                "table_name": table_name,
                "topic_name": topic_name,
                "profile_id": profile_id,
                "time_column": table_schema.time_column,
                "value_columns": table_schema.value_columns,
                "metadata_columns": table_schema.metadata_columns,
                "json_schema": json.dumps(sql_json_schema, indent=2)
                .replace("\n", "\\n")
                .replace('"', '\\"'),
            }
    elif args.source_type == "prometheus_remote_write":
        template_vars["base_port"] = args.prometheus_base_port
        template_vars["parallelism"] = args.parallelism
        template_vars["path"] = args.prometheus_path
        template_vars["bind_ip"] = args.prometheus_bind_ip

        # For optimized source, build metrics array from metrics_dict
        if args.prometheus_remote_write_source == "optimized":
            if metrics_dict is None:
                raise ValueError("metrics_dict is required for optimized source")

            # Build metrics array: [{"name": "cpu_usage", "labels": ["instance", "job"]}, ...]
            metrics_array = [
                {"name": metric_name, "labels": labels}
                for metric_name, labels in metrics_dict.items()
            ]
            template_vars["metrics_json"] = json.dumps(metrics_array)
            del template_vars["label_fields"]
        #    # Create a minimal JSON schema (won't be used by connector but required by API)
        #    minimal_schema = {
        #        "type": "object",
        #        "properties": {
        #            "metric_name": {"type": "string"},
        #            "timestamp": {"type": "integer"},
        #            "value": {"type": "number"},
        #        },
        #    }
        #    template_vars["json_schema"] = (
        #        json.dumps(minimal_schema, indent=2)
        #        .replace("\n", "\\n")
        #        .replace('"', '\\"')
        #    )
    elif args.source_type == "file":
        # NOTE: Currently assumes value_columns are F64/DOUBLE and metadata_columns are String/TEXT.
        # If more precise type mappings are needed, extend SQLTableConfig with per-column type info.
        ts_format_to_primitive = {
            "unix_millis": "UnixMillis",
            "unix_seconds": "UnixMillis",
            "rfc3339": "UnixNanos",
        }
        template_vars = {
            "table_name": table_name,
            "file_path": args.input_file_path,
            "file_format": args.file_format,
            "timestamp_field": table_schema.time_column,
            "ts_format": args.ts_format,
            "time_column": table_schema.time_column,
            "timestamp_primitive": ts_format_to_primitive[args.ts_format],
            "value_columns": table_schema.value_columns,
            "metadata_columns": table_schema.metadata_columns,
        }

    rendered = template.render(**template_vars)

    # Save to file
    filename = "connection_table_source.json"
    output_path = os.path.join(args.output_dir, filename)
    with open(output_path, "w") as f:
        f.write(rendered)

    print(f"Created source table at: {output_path}")

    if args.dry_run:
        print(f"[DRY RUN] Would create source connection table: {table_name}")
        return

    # If API URL provided, create connection table via API
    http_utils.create_arroyo_resource(
        arroyo_url=args.arroyo_url,
        endpoint="connection_tables",
        data=rendered,
        resource_type="source table",
    )


def create_sink_connection_table(
    args,
    topic_name,
    table_name,
    profile_id,
    template_dir,
):
    """Create a connection table JSON (sink) based on template"""

    template = jinja_utils.load_template(template_dir, "connection_table_sink.j2")

    rendered = template.render(
        table_name=table_name, topic_name=topic_name, profile_id=profile_id
    )

    # Save to file
    filename = "connection_table_sink.json"
    output_path = os.path.join(args.output_dir, filename)
    with open(output_path, "w") as f:
        f.write(rendered)

    print(f"Created sink table at: {output_path}")

    if args.dry_run:
        print(f"[DRY RUN] Would create sink connection table: {table_name}")
        return

    # If API URL provided, create connection table via API
    http_utils.create_arroyo_resource(
        arroyo_url=args.arroyo_url,
        endpoint="connection_tables",
        data=rendered,
        resource_type="sink table",
    )


def delete_connection_table(args, table_name):
    if args.dry_run:
        print(f"[DRY RUN] Would delete connection table: {table_name}")
        return

    # list all connection tables
    response = http_utils.make_api_request(
        url=f"{args.arroyo_url}/connection_tables",
        method="get",
    )
    response = json.loads(response)

    # get the ID of the connection table with table_name
    tables = [table for table in response["data"] if table["name"] == table_name]
    if len(tables) == 0:
        print(f"No connection table found with name {table_name}")
        return

    # delete the connection table with the ID
    for table in tables:
        http_utils.make_api_request(
            url=f"{args.arroyo_url}/connection_tables/{table['id']}",
            method="delete",
        )


def create_pipeline(
    args: argparse.Namespace,
    sql_queries: List[str],
    agg_functions_with_params: List[Tuple[str, dict]],
    streaming_aggregation_configs: List,
    json_template_dir: str,
    udf_dir: str,
):
    """Create a pipeline JSON based on template"""

    # Escape newlines in SQL query for JSON compatibility
    # Escape characters that would break the JSON string in pipeline.j2.
    # Double quotes appear now that column names are quoted (issue #116).
    sql_queries = [
        sql_query.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")
        for sql_query in sql_queries
    ]
    sql_query = "\\n\\n".join(sql_queries)

    # UDFs handling
    udfs = []
    # NOTE: if we're using Arroyo built from source (v0.15.0-dev), we can directly support &str arguments in UDAFs, and thus don't need string_to_hash
    # udf_names = list(set(agg_functions)) + ["string_to_hash"]
    unique_agg_functions = list(
        set([agg_func for agg_func, _ in agg_functions_with_params])
    )
    udf_names = unique_agg_functions + ["gzip_compress"]
    # udf_names = list(set(agg_functions))

    # Create a mapping of agg_function to parameters for UDF rendering
    agg_function_params = {}
    for agg_func, params in agg_functions_with_params:
        if agg_func not in agg_function_params:
            agg_function_params[agg_func] = params

    # Special handling for deltasetaggregator - need separate UDF instances per aggregation_id
    deltasetaggregator_instances = []
    for config in streaming_aggregation_configs:
        if config.aggregationType.lower() == "deltasetaggregator":
            deltasetaggregator_instances.append(config.aggregationId)

    for udf_name in udf_names:
        # Special case for deltasetaggregator - generate separate UDF for each aggregation_id
        if udf_name == "deltasetaggregator_":
            for aggregation_id in deltasetaggregator_instances:
                template_path = os.path.join(udf_dir, f"{udf_name}.rs.j2")

                if os.path.exists(template_path):
                    # Render the Jinja template with aggregation_id
                    udf_template = jinja_utils.load_template(
                        udf_dir, f"{udf_name}.rs.j2"
                    )
                    udf_body = udf_template.render(aggregation_id=aggregation_id)
                    udfs.append({"definition": udf_body, "language": "rust"})
                else:
                    raise FileNotFoundError(
                        f"Template {template_path} not found for deltasetaggregator"
                    )
        else:
            # Regular UDF processing for non-deltasetaggregator UDFs
            template_path = os.path.join(udf_dir, f"{udf_name}.rs.j2")
            regular_path = os.path.join(udf_dir, f"{udf_name}.rs")

            # Get parameters for this UDF (impl_mode injected in main() for sketch UDFs)
            params = dict(agg_function_params.get(udf_name, {}))

            if len(params) > 0 and not os.path.exists(template_path):
                raise ValueError(
                    f"UDF {udf_name} requires parameters {params} but no template found at {template_path}"
                )

            if os.path.exists(template_path):
                # Read template source and get required parameters
                with open(template_path, "r") as file:
                    template_source = file.read()

                # Render the Jinja template with parameters
                udf_template = jinja_utils.load_template(udf_dir, f"{udf_name}.rs.j2")

                # Get all required template variables
                required_params = jinja_utils.get_template_variables(
                    template_source, udf_template.environment
                )

                # Handle config key mapping (K -> k for KLL)
                if "K" in params and "k" in required_params:
                    params["k"] = params["K"]

                # Check that all required parameters are provided
                missing_params = required_params - set(params.keys())
                if missing_params:
                    raise ValueError(
                        f"UDF {udf_name} requires parameters {missing_params} but they were not in the configuration"
                    )

                udf_body = udf_template.render(**params)
            elif os.path.exists(regular_path):
                # Use regular file if no template exists
                with open(regular_path, "r") as f:
                    udf_body = f.read()
            else:
                raise FileNotFoundError(
                    f"Neither {template_path} nor {regular_path} exists"
                )

            udfs.append({"definition": udf_body, "language": "rust"})

    # Load pipeline template
    pipeline_template = jinja_utils.load_template(json_template_dir, "pipeline.j2")

    rendered = pipeline_template.render(
        pipeline_name=args.pipeline_name,
        sql_query=sql_query,
        udfs=udfs,
        parallelism=args.parallelism,
    )

    # Save to file
    output_path = os.path.join(args.output_dir, "pipeline.json")
    with open(output_path, "w") as f:
        f.write(rendered)

    print(f"Creating pipeline at: {output_path}")

    if args.dry_run:
        pipeline_id = "dry_run_pipeline_id"
        print(f"[DRY RUN] Would create pipeline with ID: {pipeline_id}")
        return

    # If API URL provided, create pipeline via API
    response = http_utils.create_arroyo_resource(
        arroyo_url=args.arroyo_url,
        endpoint="pipelines",
        data=rendered,
        resource_type="pipeline",
    )

    response = json.loads(response)
    pipeline_id = response["id"]
    print(f"Pipeline created with ID: {pipeline_id}")

    # Write pipeline ID to file for retrieval when running with avoid_long_ssh
    pipeline_id_file = os.path.join(args.output_dir, "pipeline_id.txt")
    with open(pipeline_id_file, "w") as f:
        f.write(pipeline_id)
        f.flush()
        os.fsync(f.fileno())  # Ensure it's written to disk
    print(f"Pipeline ID written to: {pipeline_id_file}")


def delete_pipelines(args):
    if args.dry_run:
        print("[DRY RUN] Would delete all existing pipelines")
        return

    # # list all pipelines
    # response = http_utils.make_api_request(
    #     url=f"{args.arroyo_url}/pipelines",
    #     method="get",
    # )
    # response = json.loads(response)
    # if response["data"] is None:
    #     print("No pipelines found")
    #     return

    # pipeline_ids = [pipeline["id"] for pipeline in response["data"]]
    pipeline_ids = arroyo_utils.get_all_pipelines(arroyo_url=args.arroyo_url)

    arroyo_utils.stop_and_delete_pipelines(
        arroyo_url=args.arroyo_url, pipeline_ids=pipeline_ids
    )

    # # stop and delete all pipelines
    # for pipeline_id in pipeline_ids:
    #     response = http_utils.make_api_request(
    #         url=f"{args.arroyo_url}/pipelines/{pipeline_id}",
    #         method="patch",
    #         data=json.dumps({"stop": "immediate"}),
    #     )

    # time.sleep(5)
    # for pipeline_id in pipeline_ids:
    #     success = False
    #     for _ in range(num_retries):
    #         try:
    #             response = http_utils.make_api_request(
    #                 url=f"{args.arroyo_url}/pipelines/{pipeline_id}",
    #                 method="delete",
    #             )
    #             success = True
    #         except Exception as e:
    #             print(f"Failed to delete pipeline {pipeline_id}: {e}")
    #             time.sleep(5)

    #         if not success:
    #             raise Exception(
    #                 f"Failed to delete pipeline {pipeline_id} after {num_retries} retries"
    #             )


def _quote_col(label_prefix: str, label: str) -> str:
    """Return a SQL column reference with double-quotes for case-sensitivity.

    DataFusion (Arroyo's SQL engine) normalises unquoted identifiers to
    lowercase, which silently corrupts mixed-case column names.  Wrapping
    every user-supplied identifier in double-quotes preserves the original
    casing.

    Examples:
        _quote_col("", "hostName")        -> '"hostName"'
        _quote_col("labels.", "hostName") -> 'labels."hostName"'
    """
    return f'{label_prefix}"{label}"'


def get_sql_query(
    streaming_aggregation_config: StreamingAggregationConfig,
    schema_config,  # MetricConfig or SQLTableConfig
    query_language: str,
    sql_template: Template,
    source_table: str,
    sink_table: str,
    source_type: str,
    use_nested_labels: bool,
    filter_metric_name: str = None,
) -> Tuple[str, str, dict]:

    window_type = streaming_aggregation_config.windowType
    window_size = "{} seconds".format(streaming_aggregation_config.windowSize)
    window_interval = window_size
    slide_interval = "{} seconds".format(streaming_aggregation_config.slideInterval)

    logger.info(
        f"Preparing SQL query for aggregation {streaming_aggregation_config.aggregationId}: "
        f"windowType={window_type}, windowSize={window_size}, slideInterval={slide_interval}"
    )

    agg_function = "{}_{}".format(
        streaming_aggregation_config.aggregationType,
        streaming_aggregation_config.aggregationSubType,
    )

    # Get column names based on query language
    if query_language == "sql":
        time_column = schema_config.get_time_column(
            streaming_aggregation_config.table_name
        )
        value_column = streaming_aggregation_config.value_column
        label_prefix = ""  # SQL mode: no nesting
    else:
        time_column = "timestamp"
        value_column = "value"
        label_prefix = "labels." if use_nested_labels else ""

    # Double-quote all user-supplied column names so DataFusion preserves
    # their original casing (issue #116).
    fully_qualified_group_by_columns = [
        _quote_col(label_prefix, label)
        for label in streaming_aggregation_config.labels["grouping"].keys
    ]
    fully_qualified_agg_columns = [
        _quote_col(label_prefix, label)
        for label in streaming_aggregation_config.labels["aggregated"].keys
    ]

    # Get all labels for this aggregation
    if query_language == "sql":
        source_identifier = streaming_aggregation_config.table_name
        all_labels = schema_config.get_metadata_columns(source_identifier)
    else:
        source_identifier = streaming_aggregation_config.metric
        all_labels = schema_config.config[source_identifier].keys

    all_labels_agg_columns = [_quote_col(label_prefix, label) for label in all_labels]

    # Quote the scalar column references for the same reason.
    time_column = f'"{time_column}"'
    value_column = f'"{value_column}"'

    # Determine if timestamps should be included as argument
    include_timestamps_as_argument = (
        streaming_aggregation_config.aggregationType == "multipleincrease"
    )

    # This is just a patch for topk query.
    if streaming_aggregation_config.aggregationSubType == "topk":
        key_list = all_labels_agg_columns
    else:
        key_list = fully_qualified_agg_columns
    agg_columns = ", ".join(key_list)

    sql_query = sql_template.render(
        aggregation_id=streaming_aggregation_config.aggregationId,
        sink_table=sink_table,
        agg_function=agg_function,
        agg_columns=agg_columns,
        source_table=source_table,
        group_by_columns=", ".join(fully_qualified_group_by_columns),
        window_interval=window_interval,
        window_type=window_type,  # NEW: for sliding/tumbling selection
        window_size=window_size,  # NEW: for HOP window size
        slide_interval=slide_interval,  # NEW: for HOP slide interval
        include_timestamps_as_argument=include_timestamps_as_argument,
        source_type=source_type,
        filter_metric_name=filter_metric_name,  # NEW: for multi-metric filtering
        time_column=time_column,  # NEW: for SQL mode
        value_column=value_column,  # NEW: for SQL mode
    )

    return sql_query, agg_function, streaming_aggregation_config.parameters


def build_sql_json_schema(table_schema: TableSchema) -> dict:
    """Build JSON schema for SQL-style Kafka data."""
    properties = {
        table_schema.time_column: {
            "type": "string",
            "format": "date-time",
            "description": "Timestamp column",
        }
    }
    required = [table_schema.time_column]

    for value_col in table_schema.value_columns:
        properties[value_col] = {
            "type": "number",
            "description": f"Value column: {value_col}",
        }
        required.append(value_col)

    for meta_col in table_schema.metadata_columns:
        properties[meta_col] = {
            "type": "string",
            "description": f"Metadata column: {meta_col}",
        }
        required.append(meta_col)

    return {
        "type": "object",
        "required": required,
        "properties": properties,
        "additionalProperties": False,
    }


def get_source_table_name_sql(args, table_name: str) -> str:
    """Get the source table name for SQL mode."""
    if args.source_type == "kafka":
        return f"{args.input_kafka_topic}_{table_name.replace(' ', '_')}"
    elif args.source_type == "file":
        filename = os.path.basename(args.input_file_path)
        filename_no_ext = os.path.splitext(filename)[0]
        return f"{filename_no_ext}_{table_name.replace(' ', '_')}"
    else:
        raise ValueError(f"Unsupported source type for SQL mode: {args.source_type}")


def get_source_table_name(args, metric_name):
    """Get the source table name based on the metric name and source type"""
    if args.source_type == "kafka":
        return "{}_{}".format(args.input_kafka_topic, metric_name.replace(" ", "_"))
    elif args.source_type == "prometheus_remote_write":
        return "prometheus_{}_{}".format(
            args.prometheus_base_port, metric_name.replace(" ", "_")
        )
    elif args.source_type == "file":
        # Use filename without extension for table name
        filename = os.path.basename(args.input_file_path)
        filename_no_ext = os.path.splitext(filename)[0]
        return "{}_{}".format(filename_no_ext, metric_name.replace(" ", "_"))
    else:
        raise ValueError(f"Unsupported source type: {args.source_type}")


def main(args):
    os.makedirs(args.output_dir, exist_ok=True)

    # source_table = args.input_kafka_topic + "_table"
    sink_table = args.output_kafka_topic + "_table"

    with open(args.config_file_path, "r") as fin:
        config = yaml.safe_load(fin)

    # Query language from command line argument (defaults to promql)
    query_language = args.query_language

    # Create appropriate schema config based on query language
    if query_language == "promql":
        schema_config = MetricConfig(config["metrics"])
    elif query_language == "sql":
        schema_config = SQLTableConfig(config)
    else:
        raise ValueError(f"Unsupported query_language: {query_language}")

    streaming_aggregation_configs = [
        StreamingAggregationConfig.from_dict(aggregation_config)
        for aggregation_config in config["aggregations"]
    ]

    for streaming_aggregation_config in streaming_aggregation_configs:
        streaming_aggregation_config.aggregationType = (
            streaming_aggregation_config.aggregationType.lower()
        )
        streaming_aggregation_config.aggregationSubType = (
            streaming_aggregation_config.aggregationSubType.lower()
        )
        streaming_aggregation_config.validate(schema_config, query_language)

    json_template_dir = os.path.join(args.template_dir, "json")
    sql_template_dir = os.path.join(args.template_dir, "sql")
    udf_dir = os.path.join(args.template_dir, "udfs")

    # Create connection profile for Kafka, since we definitely need it for sink
    delete_connection_profile(args)
    profile_id = create_connection_profile(args, json_template_dir)

    # For prometheus_remote_write optimized source, create ONE source for ALL metrics
    if (
        args.source_type == "prometheus_remote_write"
        and args.prometheus_remote_write_source == "optimized"
    ):
        # Create single source table for all metrics
        source_table = f"prometheus_{args.prometheus_base_port}_all_metrics"
        delete_connection_table(args, source_table)

        # Build metrics dict: {metric_name: [label1, label2, ...]}
        metrics_dict = {
            metric_name: list(metric_labels.keys)
            for metric_name, metric_labels in schema_config.config.items()
        }

        create_source_connection_table(
            args,
            None,  # topic_name not needed
            source_table,
            profile_id,
            [],  # metric_labels not used for multi-metric
            json_template_dir,
            query_language=query_language,
            metrics_dict=metrics_dict,
        )
    elif query_language == "sql":
        # SQL mode: create one source per table
        for table_name, table_schema in schema_config.config.items():
            source_table = get_source_table_name_sql(args, table_name)
            delete_connection_table(args, source_table)

            create_source_connection_table(
                args,
                args.input_kafka_topic,
                source_table,
                profile_id,
                [],  # metric_labels not used for SQL mode
                json_template_dir,
                query_language=query_language,
                table_schema=table_schema,
            )
    else:
        # For other sources (Kafka, non-optimized prometheus, file), create one source per metric
        for metric_name, metric_labels in schema_config.config.items():
            source_table = get_source_table_name(args, metric_name)
            delete_connection_table(args, source_table)

            # Set topic_name based on source type (only needed for Kafka)
            topic_name = args.input_kafka_topic if args.source_type == "kafka" else None

            create_source_connection_table(
                args,
                topic_name,
                source_table,
                profile_id,
                metric_labels.keys,
                json_template_dir,
                query_language=query_language,
            )

    delete_connection_table(args, sink_table)
    create_sink_connection_table(
        args, args.output_kafka_topic, sink_table, profile_id, json_template_dir
    )

    aggregation_sql_template = jinja_utils.load_template(
        sql_template_dir, "single_windowed_aggregation.j2"
    )
    labels_sql_template = jinja_utils.load_template(
        sql_template_dir, "distinct_windowed_labels.j2"
    )
    deltasetaggregator_sql_template = jinja_utils.load_template(
        sql_template_dir, "distinct_windowed_labels_deltasetaggregator.j2"
    )
    value_only_sql_template = jinja_utils.load_template(
        sql_template_dir, "single_arg_value_aggregation.j2"
    )

    sql_queries = []
    agg_functions_with_params = []

    # Determine if using single unified source table
    use_unified_source_table = (
        args.source_type == "prometheus_remote_write"
        and args.prometheus_remote_write_source == "optimized"
    )

    for streaming_aggregation_config in streaming_aggregation_configs:
        if use_unified_source_table:
            # Use the unified table for all metrics
            source_table = f"prometheus_{args.prometheus_base_port}_all_metrics"
        elif query_language == "sql":
            source_table = get_source_table_name_sql(
                args, streaming_aggregation_config.table_name
            )
        else:
            source_table = get_source_table_name(
                args, streaming_aggregation_config.metric
            )

        is_labels_accumulator: bool = (
            streaming_aggregation_config.aggregationType == "setaggregator"
            or streaming_aggregation_config.aggregationType == "deltasetaggregator"
        )

        # Value-only aggregations that only take Vec<f64> as a single argument
        is_value_only_aggregation: bool = (
            streaming_aggregation_config.aggregationType == "datasketcheskll"
        )

        # Choose appropriate SQL template
        if streaming_aggregation_config.aggregationType == "deltasetaggregator":
            sql_template = deltasetaggregator_sql_template
        elif is_labels_accumulator:
            sql_template = labels_sql_template
        elif is_value_only_aggregation:
            sql_template = value_only_sql_template
        else:
            sql_template = aggregation_sql_template

        # Determine if we should use nested labels based on source configuration
        # SQL mode uses flat schema (no nesting), prometheus optimized also uses flat
        use_nested_labels = not (
            query_language == "sql"
            or (
                args.source_type == "prometheus_remote_write"
                and args.prometheus_remote_write_source == "optimized"
            )
        )

        # When using unified source table, pass metric name for WHERE clause filtering
        filter_metric_name = (
            streaming_aggregation_config.metric if use_unified_source_table else None
        )

        sql_query, agg_function, parameters = get_sql_query(
            streaming_aggregation_config,
            schema_config,
            query_language,
            sql_template,
            source_table,
            sink_table,
            args.source_type,
            use_nested_labels,
            filter_metric_name,
        )

        parameters = dict(parameters)
        if agg_function in ("countminsketch_count", "countminsketch_sum"):
            parameters["impl_mode"] = getattr(
                args, "sketch_cms_impl", "legacy"
            ).capitalize()
        elif agg_function == "countminsketchwithheap_topk":
            parameters["impl_mode"] = getattr(
                args, "sketch_cmwh_impl", "legacy"
            ).capitalize()
        elif agg_function in ("datasketcheskll_", "hydrakll_"):
            parameters["impl_mode"] = getattr(
                args, "sketch_kll_impl", "legacy"
            ).capitalize()

        sql_queries.append(sql_query)
        # if not is_labels_accumulator:
        agg_functions_with_params.append((agg_function, parameters))

        print(
            "Generated SQL query for aggregation ID {}: \n{}".format(
                streaming_aggregation_config.aggregationId, sql_query
            )
        )
    delete_pipelines(args)
    create_pipeline(
        args,
        sql_queries,
        agg_functions_with_params,
        streaming_aggregation_configs,
        json_template_dir,
        udf_dir,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    # Dry run option
    parser.add_argument(
        "--dry_run",
        action="store_true",
        help="Test the logic without making API calls",
    )

    # StreamingConfig
    parser.add_argument(
        "--config_file_path",
        type=str,
        required=True,
        help="Path to the configuration file",
    )

    # Connection profile parameters
    parser.add_argument(
        "--profile_name",
        default="default-kafka-profile",
        help="Name for the connection profile",
    )
    parser.add_argument(
        "--bootstrap_servers", default="localhost:9092", help="Kafka bootstrap servers"
    )

    # Source type selection
    parser.add_argument(
        "--source_type",
        type=str,
        choices=["kafka", "prometheus_remote_write", "file"],
        required=True,
        help="Type of source to use",
    )

    # Connection table parameters
    parser.add_argument(
        "--input_kafka_topic", type=str, required=False, help="Input Kafka topic"
    )
    parser.add_argument(
        "--input_file_path", type=str, required=False, help="Path to the input file"
    )
    parser.add_argument(
        "--file_format",
        type=str,
        required=False,
        choices=["json", "parquet"],
        help="Format of the input file (required for file source)",
    )
    parser.add_argument(
        "--ts_format",
        type=str,
        required=False,
        choices=["unix_millis", "unix_seconds", "rfc3339"],
        help="Timestamp format in the input file (required for file source)",
    )

    # Prometheus remote write source parameters
    parser.add_argument(
        "--prometheus_base_port",
        type=int,
        required=False,
        help="Base port for Prometheus remote write endpoint",
    )
    parser.add_argument(
        "--prometheus_path",
        type=str,
        required=False,
        help="Path for Prometheus remote write endpoint",
    )
    parser.add_argument(
        "--prometheus_bind_ip",
        type=str,
        required=False,
        help="IP address to bind Prometheus remote write endpoint to",
    )
    parser.add_argument(
        "--parallelism",
        type=int,
        required=True,
        help="Pipeline parallelism (number of parallel tasks)",
    )
    parser.add_argument(
        "--prometheus_remote_write_source",
        type=str,
        choices=["v1", "optimized"],
        default="v1",
        help="Version of Prometheus remote_write source (v1=nested labels, optimized=flattened labels)",
    )

    parser.add_argument(
        "--output_kafka_topic", type=str, required=False, help="Output Kafka topic"
    )
    parser.add_argument(
        "--output_file_path", type=str, required=False, help="Path to the output file"
    )

    parser.add_argument(
        "--kafka_input_format",
        required=False,
        choices=["json", "avro-json", "avro-binary"],
    )
    parser.add_argument("--output_format", required=True, choices=["json", "byte"])

    parser.add_argument("--pipeline_name", required=True, help="Pipeline name")

    parser.add_argument(
        "--template_dir",
        default="./templates",
        help="Directory containing template files",
    )

    parser.add_argument(
        "--output_dir",
        default="./outputs",
        help="Directory to save the generated files",
    )

    parser.add_argument(
        "--arroyo_url",
        default="http://localhost:5115/api/v1",
        help="URL of the Arroyo API server",
    )

    parser.add_argument(
        "--query_language",
        type=str,
        choices=["promql", "sql"],
        default="promql",
        help="Query language for schema interpretation (default: promql)",
    )

    # Sketch implementation mode - must match QueryEngine (--sketch-cms-impl etc.)
    parser.add_argument(
        "--sketch_cms_impl",
        type=str,
        choices=["legacy", "sketchlib"],
        default="sketchlib",
        help="Count-Min Sketch backend (legacy | sketchlib). Must match QueryEngine.",
    )
    parser.add_argument(
        "--sketch_kll_impl",
        type=str,
        choices=["legacy", "sketchlib"],
        default="legacy",
        help="KLL Sketch backend (legacy | sketchlib). Must match QueryEngine.",
    )
    parser.add_argument(
        "--sketch_cmwh_impl",
        type=str,
        choices=["legacy", "sketchlib"],
        default="legacy",
        help="Count-Min-With-Heap backend (legacy | sketchlib). Must match QueryEngine.",
    )

    args = parser.parse_args()
    check_args(args)
    main(args)
