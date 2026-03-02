from dataclasses import dataclass
from typing import List, Optional, Union, Dict


@dataclass
class PromQLPatternBuilder:
    @staticmethod
    def any():
        return None

    @staticmethod
    def binary_op(op: str, left, right, collect_as: Optional[str] = None):
        return {
            "type": "BinaryExpr",
            "op": op,
            "left": left,
            "right": right,
            "_collect_as": collect_as,  # If set, store the binary operation details
        }

    @staticmethod
    def metric(
        name: Optional[str] = None,
        labels: Optional[Dict[str, str]] = None,
        at: Optional[str] = None,
        collect_as: Optional[str] = None,
    ):
        return {
            "type": "VectorSelector",
            "name": name,
            "matchers": labels,
            "at": at,  # Add the "@" modifier
            "_collect_as": collect_as,  # If set, store the matched metric details
        }

    @staticmethod
    def function(
        name: Union[str, List[str]],
        *args,
        collect_args_as: Optional[str] = None,
        collect_as: Optional[str] = None,
    ):
        if isinstance(name, str):
            name = [name]
        return {
            "type": "Call",
            "func": {"type": "Function", "name": name},
            "args": list(args),
            "_collect_args_as": collect_args_as,  # If set, store the function arguments
            "_collect_as": collect_as,  # If set, store the function details
        }

    @staticmethod
    def subquery(
        expr, duration: Optional[str] = None, collect_as: Optional[str] = None
    ):
        return {
            "type": "SubqueryExpr",
            "expr": expr,
            "range": duration,
            "step": None,
            "offset": None,
            "_collect_as": collect_as,  # If set, store the range details
        }

    @staticmethod
    def matrix_selector(
        vector_selector, range: Optional[str] = None, collect_as: Optional[str] = None
    ):
        """Match a matrix selector (range vector selector)"""
        return {
            "type": "MatrixSelector",
            "vector_selector": vector_selector,
            "range": range,  # e.g., '5m', '1h'
            "_collect_as": collect_as,
        }

    @staticmethod
    def aggregation(
        op: Union[str, List[str]],
        expr,
        param=None,
        by: Optional[List[str]] = None,
        without: Optional[List[str]] = None,
        collect_as: Optional[str] = None,
    ):
        if isinstance(op, str):
            op = [op]

        return {
            "type": "AggregateExpr",
            "op": op,
            "expr": expr,
            "param": param,
            "modifier": by or without or None,
            "_collect_as": collect_as,  # If set, store the aggregation details
        }

    @staticmethod
    def number(value: Optional[float] = None, collect_as: Optional[str] = None):
        return {
            "type": "NumberLiteral",
            "value": value,
            "_collect_as": collect_as,  # If set, store the number value
        }
