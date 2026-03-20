"""
Unit tests for SQL schema support in ArroyoSketch.

Tests cover:
1. SQLTableConfig parsing and validation
2. StreamingAggregationConfig SQL mode support
3. Backward compatibility with PromQL configs
"""

import pytest
import sys
import os

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


class TestSQLTableConfig:
    """Tests for SQLTableConfig class."""

    def test_parse_single_table(self):
        """Test parsing a single table from sql_schema."""
        yaml_dict = {
            "tables": [
                {
                    "name": "cpu_metrics",
                    "time_column": "timestamp",
                    "value_columns": ["cpu_usage", "cpu_system"],
                    "metadata_columns": ["host", "region"],
                }
            ]
        }

        config = SQLTableConfig(yaml_dict)

        assert "cpu_metrics" in config.config
        table = config.get_table("cpu_metrics")
        assert table.time_column == "timestamp"
        assert table.value_columns == ["cpu_usage", "cpu_system"]
        assert table.metadata_columns == ["host", "region"]

    def test_parse_multiple_tables(self):
        """Test parsing multiple tables from sql_schema."""
        yaml_dict = {
            "tables": [
                {
                    "name": "cpu_metrics",
                    "time_column": "ts",
                    "value_columns": ["cpu_usage"],
                    "metadata_columns": ["host"],
                },
                {
                    "name": "memory_metrics",
                    "time_column": "event_time",
                    "value_columns": ["memory_used", "memory_free"],
                    "metadata_columns": ["host", "datacenter"],
                },
            ]
        }

        config = SQLTableConfig(yaml_dict)

        assert len(config.config) == 2
        assert "cpu_metrics" in config.config
        assert "memory_metrics" in config.config

        cpu_table = config.get_table("cpu_metrics")
        assert cpu_table.time_column == "ts"

        mem_table = config.get_table("memory_metrics")
        assert mem_table.time_column == "event_time"
        assert mem_table.value_columns == ["memory_used", "memory_free"]

    def test_get_time_column(self):
        """Test get_time_column helper method."""
        yaml_dict = {
            "tables": [
                {
                    "name": "test_table",
                    "time_column": "custom_timestamp",
                    "value_columns": ["val"],
                    "metadata_columns": ["label"],
                }
            ]
        }

        config = SQLTableConfig(yaml_dict)
        assert config.get_time_column("test_table") == "custom_timestamp"

    def test_get_metadata_columns(self):
        """Test get_metadata_columns helper method."""
        yaml_dict = {
            "tables": [
                {
                    "name": "test_table",
                    "time_column": "ts",
                    "value_columns": ["val"],
                    "metadata_columns": ["host", "region", "cluster"],
                }
            ]
        }

        config = SQLTableConfig(yaml_dict)
        assert config.get_metadata_columns("test_table") == [
            "host",
            "region",
            "cluster",
        ]

    def test_get_nonexistent_table(self):
        """Test that get_table returns None for nonexistent table."""
        yaml_dict = {"tables": []}
        config = SQLTableConfig(yaml_dict)
        assert config.get_table("nonexistent") is None

    def test_empty_tables_list(self):
        """Test parsing with empty tables list."""
        yaml_dict = {"tables": []}
        config = SQLTableConfig(yaml_dict)
        assert len(config.config) == 0

    def test_missing_tables_key(self):
        """Test parsing with missing tables key."""
        yaml_dict = {}
        config = SQLTableConfig(yaml_dict)
        assert len(config.config) == 0


class TestStreamingAggregationConfigSQL:
    """Tests for StreamingAggregationConfig SQL mode support."""

    def test_parse_sql_aggregation_config(self):
        """Test parsing aggregation config with SQL fields."""
        config_dict = {
            "aggregationId": 1,
            "aggregationType": "MultipleSum",
            "aggregationSubType": "sum",
            "table_name": "cpu_metrics",
            "value_column": "cpu_usage",
            "labels": {
                "grouping": ["host", "region"],
                "aggregated": ["cluster"],
                "rollup": [],
            },
            "parameters": {},
            "spatialFilter": "",
            "tumblingWindowSize": 10,
        }

        agg_config = StreamingAggregationConfig.from_dict(config_dict)

        assert agg_config.table_name == "cpu_metrics"
        assert agg_config.value_column == "cpu_usage"
        assert agg_config.metric is None

    def test_default_value_column(self):
        """Test that value_column defaults to 'value' if not specified."""
        config_dict = {
            "aggregationId": 1,
            "aggregationType": "MultipleSum",
            "aggregationSubType": "sum",
            "table_name": "cpu_metrics",
            "labels": {
                "grouping": [],
                "aggregated": ["host"],
                "rollup": [],
            },
            "parameters": {},
            "spatialFilter": "",
            "tumblingWindowSize": 10,
        }

        agg_config = StreamingAggregationConfig.from_dict(config_dict)
        assert agg_config.value_column == "value"

    def test_get_source_identifier_sql(self):
        """Test get_source_identifier returns table_name for SQL mode."""
        config_dict = {
            "aggregationId": 1,
            "aggregationType": "MultipleSum",
            "aggregationSubType": "sum",
            "table_name": "my_table",
            "value_column": "val",
            "labels": {
                "grouping": [],
                "aggregated": ["label"],
                "rollup": [],
            },
            "parameters": {},
            "spatialFilter": "",
            "tumblingWindowSize": 10,
        }

        agg_config = StreamingAggregationConfig.from_dict(config_dict)
        assert agg_config.get_source_identifier() == "my_table"

    def test_validate_sql_mode_success(self):
        """Test successful validation in SQL mode."""
        config = {
            "tables": [
                {
                    "name": "cpu_metrics",
                    "time_column": "ts",
                    "value_columns": ["cpu_usage", "cpu_system"],
                    "metadata_columns": ["host", "region"],
                }
            ]
        }
        schema_config = SQLTableConfig(config)

        config_dict = {
            "aggregationId": 1,
            "aggregationType": "MultipleSum",
            "aggregationSubType": "sum",
            "table_name": "cpu_metrics",
            "value_column": "cpu_usage",
            "labels": {
                "grouping": ["host"],
                "aggregated": ["region"],
                "rollup": [],
            },
            "parameters": {},
            "spatialFilter": "",
            "tumblingWindowSize": 10,
        }

        agg_config = StreamingAggregationConfig.from_dict(config_dict)
        # Should not raise
        agg_config.validate(schema_config, query_language="sql")

    def test_validate_sql_mode_missing_table(self):
        """Test validation fails when table doesn't exist."""
        config = {"tables": []}
        schema_config = SQLTableConfig(config)

        config_dict = {
            "aggregationId": 1,
            "aggregationType": "MultipleSum",
            "aggregationSubType": "sum",
            "table_name": "nonexistent_table",
            "value_column": "val",
            "labels": {
                "grouping": [],
                "aggregated": ["label"],
                "rollup": [],
            },
            "parameters": {},
            "spatialFilter": "",
            "tumblingWindowSize": 10,
        }

        agg_config = StreamingAggregationConfig.from_dict(config_dict)

        with pytest.raises(ValueError, match="not found in sql_schema"):
            agg_config.validate(schema_config, query_language="sql")

    def test_validate_sql_mode_invalid_value_column(self):
        """Test validation fails when value_column doesn't exist in table."""
        config = {
            "tables": [
                {
                    "name": "cpu_metrics",
                    "time_column": "ts",
                    "value_columns": ["cpu_usage"],
                    "metadata_columns": ["host"],
                }
            ]
        }
        schema_config = SQLTableConfig(config)

        config_dict = {
            "aggregationId": 1,
            "aggregationType": "MultipleSum",
            "aggregationSubType": "sum",
            "table_name": "cpu_metrics",
            "value_column": "nonexistent_column",
            "labels": {
                "grouping": [],
                "aggregated": ["host"],
                "rollup": [],
            },
            "parameters": {},
            "spatialFilter": "",
            "tumblingWindowSize": 10,
        }

        agg_config = StreamingAggregationConfig.from_dict(config_dict)

        with pytest.raises(ValueError, match="value_column.*not in table"):
            agg_config.validate(schema_config, query_language="sql")

    def test_validate_sql_mode_mismatched_labels(self):
        """Test validation fails when labels don't match metadata_columns."""
        config = {
            "tables": [
                {
                    "name": "cpu_metrics",
                    "time_column": "ts",
                    "value_columns": ["cpu_usage"],
                    "metadata_columns": ["host", "region"],
                }
            ]
        }
        schema_config = SQLTableConfig(config)

        config_dict = {
            "aggregationId": 1,
            "aggregationType": "MultipleSum",
            "aggregationSubType": "sum",
            "table_name": "cpu_metrics",
            "value_column": "cpu_usage",
            "labels": {
                "grouping": ["host"],
                "aggregated": ["wrong_label"],  # doesn't match metadata_columns
                "rollup": [],
            },
            "parameters": {},
            "spatialFilter": "",
            "tumblingWindowSize": 10,
        }

        agg_config = StreamingAggregationConfig.from_dict(config_dict)

        with pytest.raises(ValueError, match="Labels do not match metadata_columns"):
            agg_config.validate(schema_config, query_language="sql")


class TestStreamingAggregationConfigPromQL:
    """Tests for backward compatibility with PromQL configs."""

    def test_parse_promql_aggregation_config(self):
        """Test parsing aggregation config with PromQL fields."""
        config_dict = {
            "aggregationId": 1,
            "aggregationType": "MultipleSum",
            "aggregationSubType": "sum",
            "metric": "fake_metric_total",
            "labels": {
                "grouping": ["instance", "job"],
                "aggregated": ["label_0"],
                "rollup": [],
            },
            "parameters": {},
            "spatialFilter": "",
            "tumblingWindowSize": 10,
        }

        agg_config = StreamingAggregationConfig.from_dict(config_dict)

        assert agg_config.metric == "fake_metric_total"
        assert agg_config.table_name is None

    def test_get_source_identifier_promql(self):
        """Test get_source_identifier returns metric for PromQL mode."""
        config_dict = {
            "aggregationId": 1,
            "aggregationType": "MultipleSum",
            "aggregationSubType": "sum",
            "metric": "my_metric",
            "labels": {
                "grouping": [],
                "aggregated": ["label"],
                "rollup": [],
            },
            "parameters": {},
            "spatialFilter": "",
            "tumblingWindowSize": 10,
        }

        agg_config = StreamingAggregationConfig.from_dict(config_dict)
        assert agg_config.get_source_identifier() == "my_metric"

    def test_validate_promql_mode_success(self):
        """Test successful validation in PromQL mode."""
        metrics = {"fake_metric": ["host", "region"]}
        metric_config = MetricConfig(metrics)

        config_dict = {
            "aggregationId": 1,
            "aggregationType": "MultipleSum",
            "aggregationSubType": "sum",
            "metric": "fake_metric",
            "labels": {
                "grouping": ["host"],
                "aggregated": ["region"],
                "rollup": [],
            },
            "parameters": {},
            "spatialFilter": "",
            "tumblingWindowSize": 10,
        }

        agg_config = StreamingAggregationConfig.from_dict(config_dict)
        # Should not raise
        agg_config.validate(metric_config, query_language="promql")

    def test_validate_promql_mode_default(self):
        """Test that query_language defaults to promql."""
        metrics = {"fake_metric": ["host"]}
        metric_config = MetricConfig(metrics)

        config_dict = {
            "aggregationId": 1,
            "aggregationType": "MultipleSum",
            "aggregationSubType": "sum",
            "metric": "fake_metric",
            "labels": {
                "grouping": [],
                "aggregated": ["host"],
                "rollup": [],
            },
            "parameters": {},
            "spatialFilter": "",
            "tumblingWindowSize": 10,
        }

        agg_config = StreamingAggregationConfig.from_dict(config_dict)
        # Should not raise - defaults to promql
        agg_config.validate(metric_config, query_language="promql")


class TestTableSchema:
    """Tests for TableSchema dataclass."""

    def test_table_schema_creation(self):
        """Test creating a TableSchema."""
        schema = TableSchema(
            time_column="ts",
            value_columns=["val1", "val2"],
            metadata_columns=["label1", "label2"],
        )

        assert schema.time_column == "ts"
        assert schema.value_columns == ["val1", "val2"]
        assert schema.metadata_columns == ["label1", "label2"]

    def test_table_schema_equality(self):
        """Test TableSchema equality."""
        schema1 = TableSchema(
            time_column="ts",
            value_columns=["val"],
            metadata_columns=["label"],
        )
        schema2 = TableSchema(
            time_column="ts",
            value_columns=["val"],
            metadata_columns=["label"],
        )

        assert schema1 == schema2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
