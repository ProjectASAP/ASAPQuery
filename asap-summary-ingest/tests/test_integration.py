"""
Integration tests for SQL schema support in ArroyoSketch.

Tests cover:
1. Helper functions (build_sql_json_schema, get_source_table_name_sql)
2. get_sql_query with SQL mode
3. End-to-end config parsing
"""

import pytest
import sys
import os
import yaml

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from promql_utilities.streaming_config.SQLTableConfig import (  # noqa: E402
    SQLTableConfig,
    TableSchema,
)
from promql_utilities.streaming_config.StreamingAggregationConfig import (  # noqa: E402
    StreamingAggregationConfig,
)
from promql_utilities.streaming_config.MetricConfig import MetricConfig  # noqa: E402
from run_arroyosketch import (  # noqa: E402
    build_sql_json_schema,
    get_source_table_name_sql,
    get_sql_query,
)
from utils import jinja_utils  # noqa: E402


class TestBuildSqlJsonSchema:
    """Tests for build_sql_json_schema helper function."""

    def test_basic_schema(self):
        """Test building JSON schema for a simple table."""
        table_schema = TableSchema(
            time_column="timestamp",
            value_columns=["cpu_usage"],
            metadata_columns=["host"],
        )

        json_schema = build_sql_json_schema(table_schema)

        assert json_schema["type"] == "object"
        assert "timestamp" in json_schema["required"]
        assert "cpu_usage" in json_schema["required"]
        assert "host" in json_schema["required"]
        assert json_schema["properties"]["timestamp"]["type"] == "string"
        assert json_schema["properties"]["timestamp"]["format"] == "date-time"
        assert json_schema["properties"]["cpu_usage"]["type"] == "number"
        assert json_schema["properties"]["host"]["type"] == "string"

    def test_multiple_value_columns(self):
        """Test building JSON schema with multiple value columns."""
        table_schema = TableSchema(
            time_column="ts",
            value_columns=["val1", "val2", "val3"],
            metadata_columns=["label"],
        )

        json_schema = build_sql_json_schema(table_schema)

        assert "val1" in json_schema["required"]
        assert "val2" in json_schema["required"]
        assert "val3" in json_schema["required"]
        assert json_schema["properties"]["val1"]["type"] == "number"
        assert json_schema["properties"]["val2"]["type"] == "number"
        assert json_schema["properties"]["val3"]["type"] == "number"

    def test_multiple_metadata_columns(self):
        """Test building JSON schema with multiple metadata columns."""
        table_schema = TableSchema(
            time_column="ts",
            value_columns=["val"],
            metadata_columns=["host", "region", "datacenter"],
        )

        json_schema = build_sql_json_schema(table_schema)

        assert "host" in json_schema["required"]
        assert "region" in json_schema["required"]
        assert "datacenter" in json_schema["required"]
        for col in ["host", "region", "datacenter"]:
            assert json_schema["properties"][col]["type"] == "string"

    def test_additional_properties_false(self):
        """Test that additionalProperties is set to False."""
        table_schema = TableSchema(
            time_column="ts",
            value_columns=["val"],
            metadata_columns=["label"],
        )

        json_schema = build_sql_json_schema(table_schema)
        assert json_schema["additionalProperties"] is False


class TestGetSourceTableNameSql:
    """Tests for get_source_table_name_sql helper function."""

    def test_kafka_source(self):
        """Test source table name generation for Kafka."""

        class MockArgs:
            source_type = "kafka"
            input_kafka_topic = "test_topic"

        args = MockArgs()
        result = get_source_table_name_sql(args, "my_table")
        assert result == "test_topic_my_table"

    def test_kafka_source_with_spaces(self):
        """Test source table name generation with spaces in table name."""

        class MockArgs:
            source_type = "kafka"
            input_kafka_topic = "test_topic"

        args = MockArgs()
        result = get_source_table_name_sql(args, "my table name")
        assert result == "test_topic_my_table_name"

    def test_file_source(self):
        """Test source table name generation for file source."""

        class MockArgs:
            source_type = "file"
            input_file_path = "/data/metrics.parquet"

        args = MockArgs()
        result = get_source_table_name_sql(args, "my_table")
        assert result == "metrics_my_table"

    def test_file_source_with_spaces(self):
        """Test source table name generation for file source with spaces in table name."""

        class MockArgs:
            source_type = "file"
            input_file_path = "/data/metrics.parquet"

        args = MockArgs()
        result = get_source_table_name_sql(args, "my table name")
        assert result == "metrics_my_table_name"

    def test_unsupported_source_type(self):
        """Test that unsupported source types raise ValueError."""

        class MockArgs:
            source_type = "prometheus_remote_write"
            input_kafka_topic = "test_topic"

        args = MockArgs()
        with pytest.raises(ValueError, match="Unsupported source type for SQL mode"):
            get_source_table_name_sql(args, "my_table")


class TestGetSqlQuerySQL:
    """Tests for get_sql_query with SQL mode."""

    @pytest.fixture
    def sql_schema_config(self):
        """Create a sample SQL schema config."""
        return SQLTableConfig(
            {
                "tables": [
                    {
                        "name": "cpu_metrics",
                        "time_column": "event_time",
                        "value_columns": ["cpu_usage", "cpu_system"],
                        "metadata_columns": ["host", "region", "service"],
                    }
                ]
            }
        )

    @pytest.fixture
    def sql_agg_config(self):
        """Create a sample SQL aggregation config."""
        return StreamingAggregationConfig.from_dict(
            {
                "aggregationId": 1,
                "aggregationType": "MultipleSum",
                "aggregationSubType": "sum",
                "table_name": "cpu_metrics",
                "value_column": "cpu_usage",
                "labels": {
                    "grouping": ["host", "region"],
                    "aggregated": ["service"],
                    "rollup": [],
                },
                "parameters": {},
                "spatialFilter": "",
                "windowSize": 10,
            }
        )

    @pytest.fixture
    def sql_template(self):
        """Load the SQL template."""
        template_dir = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "templates",
            "sql",
        )
        return jinja_utils.load_template(template_dir, "single_windowed_aggregation.j2")

    def test_sql_query_uses_value_column(
        self, sql_schema_config, sql_agg_config, sql_template
    ):
        """Test that SQL query uses the correct value_column."""
        sql_agg_config.aggregationType = "multiplesum"
        sql_agg_config.aggregationSubType = "sum"

        sql_query, agg_function, params = get_sql_query(
            streaming_aggregation_config=sql_agg_config,
            schema_config=sql_schema_config,
            query_language="sql",
            sql_template=sql_template,
            source_table="test_source",
            sink_table="test_sink",
            source_type="kafka",
            use_nested_labels=False,
        )

        assert '"cpu_usage"' in sql_query
        assert '"value"' not in sql_query or '"cpu_usage"' in sql_query

    def test_sql_query_no_label_prefix(
        self, sql_schema_config, sql_agg_config, sql_template
    ):
        """Test that SQL mode doesn't use labels. prefix."""
        sql_agg_config.aggregationType = "multiplesum"
        sql_agg_config.aggregationSubType = "sum"

        sql_query, _, _ = get_sql_query(
            streaming_aggregation_config=sql_agg_config,
            schema_config=sql_schema_config,
            query_language="sql",
            sql_template=sql_template,
            source_table="test_source",
            sink_table="test_sink",
            source_type="kafka",
            use_nested_labels=False,
        )

        # Should have flat quoted column names, not labels.host
        assert "labels." not in sql_query
        # Should have double-quoted host and region directly
        assert '"host"' in sql_query
        assert '"region"' in sql_query


class TestTemplateSelectionForSetAggregatorSQL:
    """Tests for SQL SetAggregator template selection in run_arroyosketch main loop."""

    def test_sql_setaggregator_uses_value_only_template(self):
        # This mirrors the template selection branch in run_arroyosketch.main().
        query_language = "sql"
        aggregation_type = "setaggregator"
        is_labels_accumulator = aggregation_type in {"setaggregator", "deltasetaggregator"}
        is_value_only_aggregation = aggregation_type == "datasketcheskll"

        if aggregation_type == "deltasetaggregator":
            selected = "deltasetaggregator_sql_template"
        elif aggregation_type == "setaggregator" and query_language == "sql":
            selected = "value_only_sql_template"
        elif is_labels_accumulator:
            selected = "labels_sql_template"
        elif is_value_only_aggregation:
            selected = "value_only_sql_template"
        else:
            selected = "aggregation_sql_template"

        assert selected == "value_only_sql_template"


class TestGetSqlQueryPromQL:
    """Tests for get_sql_query with PromQL mode (backward compatibility)."""

    @pytest.fixture
    def promql_metric_config(self):
        """Create a sample PromQL metric config."""
        return MetricConfig({"fake_metric": ["instance", "job", "label_0"]})

    @pytest.fixture
    def promql_agg_config(self):
        """Create a sample PromQL aggregation config."""
        return StreamingAggregationConfig.from_dict(
            {
                "aggregationId": 1,
                "aggregationType": "MultipleSum",
                "aggregationSubType": "sum",
                "metric": "fake_metric",
                "labels": {
                    "grouping": ["instance", "job"],
                    "aggregated": ["label_0"],
                    "rollup": [],
                },
                "parameters": {},
                "spatialFilter": "",
                "windowSize": 10,
            }
        )

    @pytest.fixture
    def sql_template(self):
        """Load the SQL template."""
        template_dir = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "templates",
            "sql",
        )
        return jinja_utils.load_template(template_dir, "single_windowed_aggregation.j2")

    def test_promql_query_uses_value(
        self, promql_metric_config, promql_agg_config, sql_template
    ):
        """Test that PromQL query uses 'value' column."""
        promql_agg_config.aggregationType = "multiplesum"
        promql_agg_config.aggregationSubType = "sum"

        sql_query, _, _ = get_sql_query(
            streaming_aggregation_config=promql_agg_config,
            schema_config=promql_metric_config,
            query_language="promql",
            sql_template=sql_template,
            source_table="test_source",
            sink_table="test_sink",
            source_type="kafka",
            use_nested_labels=True,
        )

        assert '"value"' in sql_query

    def test_promql_query_uses_label_prefix(
        self, promql_metric_config, promql_agg_config, sql_template
    ):
        """Test that PromQL query uses labels. prefix when nested."""
        promql_agg_config.aggregationType = "multiplesum"
        promql_agg_config.aggregationSubType = "sum"

        sql_query, _, _ = get_sql_query(
            streaming_aggregation_config=promql_agg_config,
            schema_config=promql_metric_config,
            query_language="promql",
            sql_template=sql_template,
            source_table="test_source",
            sink_table="test_sink",
            source_type="kafka",
            use_nested_labels=True,
        )

        assert 'labels."instance"' in sql_query
        assert 'labels."job"' in sql_query


class TestEndToEndConfigParsing:
    """End-to-end tests for config file parsing."""

    def test_parse_sql_config_file(self):
        """Test parsing a complete SQL config file."""
        config_content = """
query_language: sql

tables:
  - name: system_metrics
    time_column: event_time
    value_columns:
      - cpu_percent
      - memory_mb
    metadata_columns:
      - hostname
      - datacenter
      - service

aggregations:
  - aggregationId: 1
    table_name: system_metrics
    value_column: cpu_percent
    aggregationType: MultipleSum
    aggregationSubType: sum
    labels:
      grouping:
        - hostname
        - datacenter
      aggregated:
        - service
      rollup: []
    parameters: {}
    spatialFilter: ''
    windowSize: 10
"""
        config = yaml.safe_load(config_content)

        assert config["query_language"] == "sql"

        schema_config = SQLTableConfig(config)
        assert "system_metrics" in schema_config.config

        agg_configs = [
            StreamingAggregationConfig.from_dict(agg) for agg in config["aggregations"]
        ]
        assert len(agg_configs) == 1
        assert agg_configs[0].table_name == "system_metrics"
        assert agg_configs[0].value_column == "cpu_percent"

        # Validate should pass
        agg_configs[0].validate(schema_config, query_language="sql")

    def test_parse_promql_config_file(self):
        """Test parsing a complete PromQL config file (backward compatibility)."""
        config_content = """
aggregations:
  - aggregationId: 1
    aggregationSubType: sum
    aggregationType: MultipleSum
    labels:
      aggregated:
        - label_0
      grouping:
        - instance
        - job
      rollup: []
    metric: fake_metric_total
    parameters: {}
    spatialFilter: ''
    windowSize: 10

metrics:
  fake_metric_total:
    - instance
    - job
    - label_0
"""
        config = yaml.safe_load(config_content)

        # No query_language means default to promql
        query_language = config.get("query_language", "promql")
        assert query_language == "promql"

        metric_config = MetricConfig(config["metrics"])
        assert "fake_metric_total" in metric_config.config

        agg_configs = [
            StreamingAggregationConfig.from_dict(agg) for agg in config["aggregations"]
        ]
        assert len(agg_configs) == 1
        assert agg_configs[0].metric == "fake_metric_total"

        # Validate should pass
        agg_configs[0].validate(metric_config, query_language="promql")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
