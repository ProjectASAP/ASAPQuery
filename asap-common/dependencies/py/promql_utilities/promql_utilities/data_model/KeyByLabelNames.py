from typing import List


class KeyByLabelNames:
    def __init__(self, keys: List[str]):
        self.keys = sorted(keys)

    def __repr__(self) -> str:
        return f"KeyByLabelNames({self.keys})"

    def __hash__(self) -> int:
        return hash(tuple(self.keys))

    def __eq__(self, other) -> bool:
        if not isinstance(other, KeyByLabelNames):
            return False
        return self.keys == other.keys

    def __add__(self, other: "KeyByLabelNames") -> "KeyByLabelNames":
        if not isinstance(other, KeyByLabelNames):
            raise ValueError("Addition is only supported for KeyByLabelNames")
        return KeyByLabelNames(list(set(self.keys) | set(other.keys)))

    def __sub__(self, other: "KeyByLabelNames") -> "KeyByLabelNames":
        if not isinstance(other, KeyByLabelNames):
            raise ValueError("Subtraction is only supported for KeyByLabelNames")
        return KeyByLabelNames(list(set(self.keys) - set(other.keys)))

    def serialize_to_json(self) -> List[str]:
        return self.keys

    @staticmethod
    def deserialize_from_json(data: List[str]) -> "KeyByLabelNames":
        return KeyByLabelNames(data)
