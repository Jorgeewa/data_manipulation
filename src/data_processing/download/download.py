from abc import ABC, abstractmethod
from typing import List, Tuple, Union, Callable, Any, Dict
from dataclasses import dataclass
import pandas as pd


@dataclass
class Download(ABC):

    cnx: Any
    round_id: str
    observable_name: str

    @abstractmethod
    def __call__(self, time: str):
        pass


@dataclass
class DownloadMainTablePandas(Download):

    cnx: Any
    round_id: str
    observable_name: str
    

    def __post_init__(self):
        self.table = f"round_data_{self.observable_name}"

    def __call__(self, time: str) -> pd.DataFrame:
        query = f"Select * from {self.table} where round_id = '{self.round_id}' and time > '{time}' order by time asc"
        return pd.read_sql(query, con=self.cnx)


@dataclass
class DownloadRound(Download):

    round_id: str
    cnx: Any

    def __call__(self, **kwargs: Dict[str, str]) -> pd.DataFrame:
        query = f"Select * from {kwargs['table']} where round_id = '{kwargs['round_id']}' and time > '{kwargs['time']}' order by time asc"
        return pd.read_sql(query, con=self.cnx)

@dataclass
class DownloadNewDay(Download):

    round_id: str
    cnx: Any

    def __call__(self, **kwargs: Dict[str, str]) -> pd.DataFrame:
        query = f"Select * from {kwargs['table']} where round_id = '{kwargs['round_id']}' and time > '{kwargs['time']}' order by time asc"
        return pd.read_sql(query, con=self.cnx)


@dataclass
class DownloadTimeofDay(Download):

    round_id: str
    cnx: Any

    def __call__(self, **kwargs: Dict[str, str]) -> pd.DataFrame:
        query = f"Select * from {kwargs['table']} where round_id = '{kwargs['round_id']}' and time > '{kwargs['time']}' order by time asc"
        return pd.read_sql(query, con=self.cnx)