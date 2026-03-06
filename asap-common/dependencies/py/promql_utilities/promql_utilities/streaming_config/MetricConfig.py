from promql_utilities.data_model.KeyByLabelNames import KeyByLabelNames


class MetricConfig:
    def __init__(self, yaml_str):
        self.config = {}
        for metric, labels in yaml_str.items():
            self.config[metric] = KeyByLabelNames(labels)

    @classmethod
    def from_list(cls, yaml_list):
        """Create MetricConfig from a list-of-dicts format used by Controller.

        Format: [{"metric": "name", "labels": ["l1", "l2"]}, ...]
        """
        as_dict = {item["metric"]: item["labels"] for item in yaml_list}
        return cls(as_dict)
