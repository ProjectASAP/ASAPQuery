"""
Rather than instantiating any of these cost models directly, it is preferred
for the user to use create_model(CostModelOption, *args) to initialize the cost model.

When implementing a new model, the abstract CostModel() class should be used
as a parent class. Once a new model is implemented, it should be added to the
CostModelOption enum and the create_model function.
"""

from abc import ABC, abstractmethod
from enum import Enum, auto
from typing import Any


# flake8: noqa
# Enum for available cost models
class CostModelOption(Enum):
    """
    Enumeration of implemented cost models.
    """

    NO_TRANSFORM = auto()
    SUM = auto()
    ARITHMETIC_AVG = auto()


class CostModel(ABC):
    """
    Abstract class representing any cost model. All implemented cost models
    must be a child class of this abstract class.
    """

    @abstractmethod
    def __init__(self):
        """
        Any initial setup for models which require it. Usually, these are
        models which maintain some sort of state
        """
        pass

    @abstractmethod
    def compute(self, x: Any) -> Any:
        """
        Absract method for updating a cost model (if it has memory). It must
        return the output of the model after updating
        """
        pass


class NoTransform(CostModel):
    """
    CostModel which applies no transformation when computing, i.e. calls to
    compute simply return the input argument
    """

    def __init__(self):
        pass

    def compute(self, x: Any):
        return x

    @property
    def name(self):
        return "NO_TRANSFORM"


# NOTE: Assumes scalar inputs (e.g. int and float)
class Sum(CostModel):
    """
    Model to represent the running sum of all samples
    """

    def __init__(self):
        self.sum = 0

    def compute(self, x: Any) -> Any:
        """
        Returns the sum of x and all previous values
        """
        if x is None:
            raise TypeError("Input argument cannot be None")
        self.sum += x
        return self.sum

    @property
    def name(self):
        return "SUM"


# NOTE: Assumes scalar inputs (e.g. int and float)
class ArithmeticAverage(CostModel):
    """
    Model to represent a running average across all samples
    """

    def __init__(self):
        self.average = 0
        self.n = 0

    def compute(self, x: Any) -> Any:
        """
        Computes and returns the new average after including x

        Updates the internal average
        """
        if x is None:
            raise TypeError("Input argument cannot be None")

        self.n += 1
        self.average = self.average * (self.n - 1) / self.n + x / self.n
        return self.average

    @property
    def name(self):
        return "ARITHMETIC_AVG"


def create_model(cost_model_option: CostModelOption, *args):
    """
    Given a CostModelOption, initialize and return the corresponding cost model.
        *args is to provide a CostModel with additional creation arguments if
        the particular model takes additional parameters during creation
    """
    if cost_model_option is None:
        raise TypeError("cost_model_option cannot be None")
    elif not isinstance(cost_model_option, type(CostModelOption.NO_TRANSFORM)):
        raise TypeError("First argument, cost_model_option, must be a CostModelOption")

    if cost_model_option == CostModelOption.NO_TRANSFORM:
        return NoTransform()
    elif cost_model_option == CostModelOption.SUM:
        return Sum()
    elif cost_model_option == CostModelOption.ARITHMETIC_AVG:
        return ArithmeticAverage()
    else:
        raise ValueError("Given cost model option not implemented.")
