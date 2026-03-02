from dataclasses import dataclass
from typing import Dict, Any
from promql_parser import (
    VectorSelector,
    MatrixSelector,
    Call,
    BinaryExpr,
    AggregateExpr,
    SubqueryExpr,
    NumberLiteral,
    TokenType,
)


@dataclass
class MatchResult:
    matches: bool
    tokens: Dict[str, Any]

    def __bool__(self):
        return self.matches


class PromQLPattern:
    """Pattern matching implementation (same as before)"""

    def __init__(self, ast_pattern: dict):
        self.pattern = ast_pattern

    def matches(self, node, debug=False) -> MatchResult:
        tokens = {}
        result = self._matches_recursive(node, self.pattern, tokens, debug)
        return MatchResult(matches=result, tokens=tokens)

    def _node_to_dict(self, node: Any) -> dict:
        """Convert a promql_parser node to a dictionary representation"""
        if isinstance(node, VectorSelector):
            return {
                "type": "VectorSelector",
                "name": node.name,
                "matchers": node.matchers,
                "at": node.at,  # Include the "@" modifier
                "ast": node,
            }
        elif isinstance(node, MatrixSelector):
            return {
                "type": "MatrixSelector",
                "vector_selector": node.vector_selector,
                "range": node.range,
                "ast": node,
            }
        elif isinstance(node, Call):
            return {
                "type": "Call",
                "func": {"type": "Function", "name": node.func.name},
                "args": node.args,
                "ast": node,
            }
        elif isinstance(node, BinaryExpr):
            return {
                "type": "BinaryExpr",
                "op": node.op,
                "left": node.lhs,
                "right": node.rhs,
                "ast": node,
            }
        elif isinstance(node, AggregateExpr):
            return {
                "type": "AggregateExpr",
                "op": str(node.op),
                "expr": node.expr,
                "param": node.param,
                "modifier": node.modifier,
                "ast": node,
            }
        elif isinstance(node, SubqueryExpr):
            return {
                "type": "SubqueryExpr",
                "expr": node.expr,
                "range": node.range,
                "step": node.step,
                "offset": node.offset,
                "ast": node,
            }
        elif isinstance(node, NumberLiteral):
            return {"type": "NumberLiteral", "value": node.val, "ast": node}
        elif isinstance(node, dict):
            return node
        else:
            raise ValueError(f"Unsupported node type: {type(node)}")

    def _matches_recursive(
        self, node, pattern: dict, tokens: dict, debug: bool
    ) -> bool:
        if pattern is None:
            return True

        # if not isinstance(node, dict) and not isinstance(node, VectorSelector):
        #    return False

        node_dict = self._node_to_dict(node)

        if debug:
            print("After return point 2")
            print(node_dict)
            print(pattern)
            print(tokens)

        if "type" in pattern and pattern["type"] != node_dict["type"]:
            return False

        if debug:
            print("After return point 3")
            print(node_dict)
            print(pattern)
            print(tokens)

        # Collect tokens if requested
        collect_as = pattern.get("_collect_as")
        if collect_as:
            if node_dict["type"] == "VectorSelector":
                tokens[collect_as] = {
                    "name": node_dict["name"],
                    "labels": node_dict["matchers"],
                    "at": node_dict["at"],
                    "ast": node_dict["ast"],
                }
            elif node_dict["type"] == "Call":
                tokens[collect_as] = {
                    "name": node_dict["func"]["name"],
                    "args": node_dict["args"],
                    "ast": node_dict["ast"],
                }
            elif node_dict["type"] == "MatrixSelector":
                tokens[collect_as] = {
                    "range": node_dict["range"],
                    "ast": node_dict["ast"],
                }
            elif node_dict["type"] == "SubqueryExpr":
                tokens[collect_as] = {
                    "range": node_dict["range"],
                    "offset": node_dict["offset"],
                    "step": node_dict["step"],
                    "ast": node_dict["ast"],
                }
            elif node_dict["type"] == "AggregateExpr":
                tokens[collect_as] = {
                    "op": node_dict["op"],
                    "modifier": node_dict["modifier"],
                    "param": node_dict["param"],
                    "ast": node_dict["ast"],
                }
            elif node_dict["type"] == "NumberLiteral":
                tokens[collect_as] = node_dict["value"]
            elif node_dict["type"] == "BinaryExpr":
                tokens[collect_as] = {
                    "op": node_dict["op"],
                    "left": node_dict["left"],
                    "right": node_dict["right"],
                    "ast": node_dict["ast"],
                }

        # Special handling for function arguments collection
        collect_args_as = pattern.get("_collect_args_as")
        if collect_args_as:
            tokens[collect_args_as] = node_dict["args"]

        for key, pattern_value in pattern.items():
            if key.startswith("_"):  # Skip our special collection directives
                continue

            if key not in node_dict:
                if debug:
                    print(f"Key {key} not found in node_dict")
                return False

            node_value = node_dict[key]

            if key in ["name", "op"] and isinstance(pattern_value, list):
                if node_value not in pattern_value:
                    if debug:
                        print(f"Failed to match {node_value} with {pattern_value}")
                    return False
                continue

            if pattern_value is None:
                continue

            if isinstance(pattern_value, dict):
                if not self._matches_recursive(
                    node_value, pattern_value, tokens, debug
                ):
                    if debug:
                        print(f"(a) Failed to match {node_value} with {pattern_value}")
                    return False
            elif isinstance(pattern_value, list):
                if not isinstance(node_value, list) or len(pattern_value) != len(
                    node_value
                ):
                    if debug:
                        print(
                            f"(b) Failed to match list {node_value} with {pattern_value}"
                        )
                    return False
                for p_item, n_item in zip(pattern_value, node_value):
                    if isinstance(p_item, dict):
                        if not self._matches_recursive(n_item, p_item, tokens, debug):
                            if debug:
                                print(f"(c) Failed to match {n_item} with {p_item}")
                            return False
                    elif p_item != n_item:
                        if debug:
                            print(f"(d) Failed to match {n_item} with {p_item}")
                        return False
            elif isinstance(node_value, TokenType):
                if pattern_value != str(node_value):
                    if debug:
                        print(
                            f"(e) Failed to match token {node_value} with {pattern_value}"
                        )
                    return False
            elif pattern_value != node_value:
                if debug:
                    print(f"(f) Failed to match {node_value} with {pattern_value}")
                return False

        return True

    # def matches(self, node) -> bool:
    #    if self.pattern is None:
    #        return True
    #
    #    if not isinstance(node, dict) and not isinstance(node, VectorSelector):
    #        return False
    #
    #    if isinstance(node, VectorSelector):
    #        node = {
    #            'type': 'VectorSelector',
    #            'name': node.name,
    #            'label_matchers': node.label_matchers
    #        }
    #
    #    if 'type' in self.pattern and self.pattern['type'] != node.get('type'):
    #        return False
    #
    #    for key, pattern_value in self.pattern.items():
    #        if key not in node:
    #            return False
    #
    #        node_value = node[key]
    #
    #        if pattern_value is None:
    #            continue
    #
    #        if isinstance(pattern_value, dict):
    #            if not self.matches(node_value):
    #                return False
    #        elif pattern_value != node_value:
    #            return False
    #
    #    return True
