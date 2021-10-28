from abc import ABC, abstractmethod
from typing import List, Tuple, Union, Callable, Any, Dict
from dataclasses import dataclass
import pandas as pd


@dataclass
class DataModification(ABC):

    round_id: str
    event: Event

    @abstractmethod
    def invalidate_round(self, round_number: int) -> None:
        pass

    @abstractmethod
    def execute_many(self, rows: List[Tuple[str]]) -> None:
        pass
