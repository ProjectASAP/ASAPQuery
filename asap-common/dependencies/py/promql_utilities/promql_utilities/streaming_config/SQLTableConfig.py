from dataclasses import dataclass
from typing import Dict, List


@dataclass
class TableSchema:
    """Schema for a single SQL table."""

    time_column: str
    value_columns: List[str]
    metadata_columns: List[str]


class SQLTableConfig:
    """
    SQL schema configuration, equivalent to MetricConfig for SQL mode.

    Mirrors the Rust SQLSchema/Table structure in:
    CommonDependencies/dependencies/rs/sql_utilities/src/ast_matching/sqlhelper.rs
    """

    def __init__(self, yaml_dict: dict):
        self.config: Dict[str, TableSchema] = {}
        for table in yaml_dict.get("tables", []):
            self.config[table["name"]] = TableSchema(
                time_column=table["time_column"],
                value_columns=table["value_columns"],
                metadata_columns=table["metadata_columns"],
            )

    def get_table(self, table_name: str) -> TableSchema:
        return self.config.get(table_name)

    def get_time_column(self, table_name: str) -> str:
        return self.config[table_name].time_column

    def get_metadata_columns(self, table_name: str) -> List[str]:
        return self.config[table_name].metadata_columns
