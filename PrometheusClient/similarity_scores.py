import numpy as np
from numpy.typing import NDArray
from typing import Any


def correlation(
    exact: NDArray[np.floating[Any]], estimate: NDArray[np.floating[Any]]
) -> float:
    return float(np.corrcoef(exact, estimate)[0, 1])


def l1_norm(
    exact: NDArray[np.floating[Any]], estimate: NDArray[np.floating[Any]]
) -> float:
    return float(np.sum(np.abs(exact - estimate)))


def l2_norm(
    exact: NDArray[np.floating[Any]], estimate: NDArray[np.floating[Any]]
) -> float:
    return float(np.sum(np.square(exact - estimate)))
