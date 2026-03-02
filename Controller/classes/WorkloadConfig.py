from typing import List

from classes.SingleQueryConfig import SingleQueryConfig


class WorkloadConfig:
    def __init__(self, singe_query_configs: List[SingleQueryConfig]):
        pass

    def remove_common_subexpressions(self):
        pass

    def get_streaming_config(self):
        pass

    def get_estimation_config(self):
        pass
