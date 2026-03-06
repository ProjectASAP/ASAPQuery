import yaml

# from ruamel.yaml import YAML

from typing import Dict, Tuple, Union
from promql_utilities.streaming_config.MetricConfig import MetricConfig
from promql_utilities.streaming_config.SQLTableConfig import SQLTableConfig
from promql_utilities.data_model.KeyByLabelNames import KeyByLabelNames

yaml.add_representer(
    KeyByLabelNames,
    lambda dumper, data: dumper.represent_list(data.serialize_to_json()),
)

# yaml_writer = YAML()
# yaml_writer.representer.add_representer(
#     KeyByLabelNames,
#     lambda dumper, data: dumper.represent_sequence(
#         "tag:yaml.org,2002:seq", data.serialize_to_json(), flow_style=False
#     ),
# )


class StreamingAggregationConfig:
    aggregationId: int
    aggregationType: str
    aggregationSubType: str

    # NEW fields for sliding window support (Issue #236)
    windowSize: int  # Window size in seconds (e.g., 900s for 15m)
    slideInterval: int  # Slide/hop interval in seconds (e.g., 30s)
    windowType: str  # "tumbling" or "sliding"

    # DEPRECATED but kept for backward compatibility
    tumblingWindowSize: int  # For reading old configs

    spatialFilter: str
    metric: str  # PromQL mode: metric name
    parameters: dict

    labels: Dict[str, KeyByLabelNames]

    # SQL-specific fields (optional, used when query_language=sql)
    table_name: str  # SQL mode: table name
    value_column: str  # SQL mode: which value column to aggregate

    def __init__(self):
        self.labels = {
            "rollup": KeyByLabelNames([]),
            "grouping": KeyByLabelNames([]),
            "aggregated": KeyByLabelNames([]),
        }
        # Default to tumbling windows for backward compatibility
        self.windowType = "tumbling"
        # SQL fields default to None
        self.table_name = None
        self.value_column = None
        self.metric = None

    @staticmethod
    def from_dict(aggregation_config: dict) -> "StreamingAggregationConfig":
        aggregation = StreamingAggregationConfig()
        aggregation.aggregationId = aggregation_config["aggregationId"]
        aggregation.aggregationType = aggregation_config["aggregationType"]
        aggregation.aggregationSubType = aggregation_config["aggregationSubType"]

        # NEW: Handle new window fields with backward compatibility
        aggregation.windowType = aggregation_config.get("windowType", "tumbling")
        aggregation.windowSize = aggregation_config.get(
            "windowSize", aggregation_config.get("tumblingWindowSize")
        )
        aggregation.slideInterval = aggregation_config.get(
            "slideInterval", aggregation_config.get("tumblingWindowSize")
        )

        # Keep deprecated field for backward compatibility
        aggregation.tumblingWindowSize = aggregation_config.get(
            "tumblingWindowSize", aggregation.windowSize
        )

        aggregation.spatialFilter = aggregation_config["spatialFilter"]
        aggregation.parameters = aggregation_config["parameters"]

        # Handle both PromQL (metric) and SQL (table_name/value_column) formats
        aggregation.metric = aggregation_config.get("metric")
        aggregation.table_name = aggregation_config.get("table_name")
        aggregation.value_column = aggregation_config.get("value_column", "value")

        for k, v in aggregation_config["labels"].items():
            if k not in aggregation.labels:
                raise ValueError(f"Invalid label name: {k}")
            if v is not None:
                aggregation.labels[k] = KeyByLabelNames(v)

        return aggregation

    def validate(
        self,
        schema_config: Union[MetricConfig, SQLTableConfig],
        query_language: str,
    ):
        """Validate against MetricConfig (promql) or SQLTableConfig (sql)."""
        configured_labels = KeyByLabelNames([])
        for k, v in self.labels.items():
            assert v is not None
            configured_labels += v

        if query_language == "promql":
            # Existing validation logic for PromQL
            if schema_config.config[self.metric] != configured_labels:
                raise ValueError(
                    "Labels do not match: {} vs {}".format(
                        schema_config.config[self.metric],
                        configured_labels,
                    )
                )
        elif query_language == "sql":
            # SQL validation: check labels match metadata_columns
            table_schema = schema_config.get_table(self.table_name)
            if table_schema is None:
                raise ValueError(f"Table '{self.table_name}' not found in sql_schema")

            expected_columns = set(table_schema.metadata_columns)
            actual_columns = set(configured_labels.keys)
            if expected_columns != actual_columns:
                raise ValueError(
                    f"Labels do not match metadata_columns for table {self.table_name}: "
                    f"expected {expected_columns}, got {actual_columns}"
                )
            # Validate value_column exists
            if self.value_column not in table_schema.value_columns:
                raise ValueError(
                    f"value_column '{self.value_column}' not in table {self.table_name} "
                    f"value_columns: {table_schema.value_columns}"
                )

    def to_dict(
        self,
        schema_config: Union[MetricConfig, SQLTableConfig],
        query_language: str,
    ) -> dict:
        self.validate(schema_config, query_language)
        return self.__dict__

    def get_source_identifier(self) -> str:
        """Get the metric name (promql) or table name (sql)."""
        return self.metric if self.metric else self.table_name

    def get_identifying_key(self) -> Tuple:
        keys = [
            self.aggregationType,
            self.aggregationSubType,
            self.windowType,  # NEW: Include window type
            self.windowSize,  # NEW: Include window size
            self.slideInterval,  # NEW: Include slide interval
            self.tumblingWindowSize,  # Keep for backward compatibility
            self.spatialFilter,
            self.metric,
            self.table_name,  # SQL mode: table name
            self.value_column,  # SQL mode: value column
            tuple(self.parameters.items()),
        ]
        for k in sorted(self.labels.keys()):
            keys.append(k)
            keys.append(tuple(self.labels[k].serialize_to_json()))

        return tuple(keys)
