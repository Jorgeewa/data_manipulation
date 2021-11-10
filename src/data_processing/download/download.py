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
    def __call__(self, **kwargs: Dict[str, str]) -> pd.DataFrame:
        pass


@dataclass
class DownloadMainTablePandas(Download):

    cnx: Any
    round_id: str
    observable_name: str
    

    def __post_init__(self):
        self.table = f"round_data_{self.observable_name}"

    def __call__(self, **kwargs: Dict[str, str]) -> pd.DataFrame:
        query = f"Select * from {self.table} where round_id = '{self.round_id}' and time > '{kwargs['time']}' order by time asc"
        return pd.read_sql(query, con=self.cnx)


@dataclass
class DownloadRound(Download):

    cnx: Any
    round_id: str
    observable_name: str

    def __post_init__(self):
        self.table = f"round_data_{self.observable_name}"

    def __call__(self, **kwargs: Dict[str, str]) -> pd.DataFrame:
        query = f"Select value, time, x, y, z, round_id, round_number, observable_name from {self.table} \
                    where round_id = '{self.round_id}' and round_number = '{kwargs['round_number']}' and \
                    observable_name = '{self.observable_name}' and day_of_production >= 0 order by time desc"
        return pd.read_sql(query, con=self.cnx)

@dataclass
class DownloadNewDayAmbientCondition(Download):

    cnx: Any
    round_id: str
    observable_name: str


    def __post_init__(self):
        self.table = f"round_data_{self.observable_name}"


    def __call__(self, **kwargs: Dict[str, str]) -> pd.DataFrame:
        query = f"SELECT round(SUM(value)/COUNT(value), 2) as value, ADDTIME(DATE_FORMAT(time, '%Y-%m-%d 00:00:00'), '12:00:00') AS time, \
                x, y, z, round_id, day_of_production, observable_name  from {self.table} where round_id = '{self.round_id}' \
                and day_of_production = '{kwargs['day']}' and day_of_production >= 0 and observable_name = '{self.observable_name}' and x != -1 GROUP BY x, y"
        return pd.read_sql(query, con=self.cnx)


@dataclass
class DownloadNewDayAnomaly(Download):

    cnx: Any
    round_id: str
    observable_name: str


    def __post_init__(self):
        self.table = f"round_data_{self.observable_name}"

    def __call__(self, **kwargs: Dict[str, str]) -> pd.DataFrame:
        query = f"SELECT max(value) as value, ADDTIME(DATE_FORMAT(time, '%Y-%m-%d 00:00:00'), '12:00:00') AS time, \
                x, y, z, round_id, day_of_production, observable_name  from {self.table} where round_id = '{self.round_id}' \
                and day_of_production = '{kwargs['day']}' and day_of_production >= 0 and observable_name = '{self.observable_name}' and x != -1 GROUP BY x, y"
        return pd.read_sql(query, con=self.cnx)

@dataclass
class DownloadTimeofDayAmbientCondition(Download):

    cnx: Any
    round_id: str
    observable_name: str

    def __post_init__(self):
        self.table = f"round_data_{self.observable_name}"

    def __call__(self, **kwargs: Dict[str, str]) -> pd.DataFrame:
        query = f"SELECT round(SUM(value)/COUNT(value), 2) as value, '{kwargs['midpoint']}' as time, x, y, z, \
                round_id, day_of_production, {kwargs['quadrant']} as time_of_day, observable_name  from {self.table} where round_id = '{self.round_id}' \
                and time >= '{kwargs['time_start']}' and time <= '{kwargs['time_end']}' and observable_name = '{self.observable_name}' and x != -1 and day_of_production >= 0 GROUP BY x, y"
        return pd.read_sql(query, con=self.cnx)

@dataclass
class DownloadTimeofDayAnomaly(Download):

    cnx: Any
    round_id: str
    observable_name: str

    def __post_init__(self):
        self.table = f"round_data_{self.observable_name}"


    def __call__(self, **kwargs: Dict[str, str]) -> pd.DataFrame:
        query = f"SELECT max(value) as value, {kwargs['midpoint']} as time, x, y, z, \
                round_id, day_of_production, {kwargs['quadrant']}, observable_name  from {self.table} where round_id = '{self.round_id}' \
                and time >= '{kwargs['time_start']}' and time <= '{kwargs['time_end']}' and observable_name = '{self.observable_name}' and x != -1 and day_of_production >= 0 GROUP BY x, y"
        return pd.read_sql(query, con=self.cnx)


@dataclass
class DownloadHourlyOverviewAmbientCondition(Download):

    cnx: Any
    round_id: str
    observable_name: str


    def __post_init__(self):
        self.table = f"round_data_{self.observable_name}"

    def __call__(self, **kwargs: Dict[str, str]) -> pd.DataFrame:
        query = f"SELECT ROUND(AVG(value), 2) as mean, ROUND(STDDEV(value), 2) as sd, null as total,\
                DATE_FORMAT(time, '%Y-%m-%d %H:00:00') as time, round_number, day_of_production, round_id ,observable_name FROM \
                {self.table} where round_id = '{self.round_id}' and time >= '{kwargs['time']}' and observable_name = '{self.observable_name}' and day_of_production >= 0 GROUP BY DATE_FORMAT(time, '%Y-%m-%d %H:00:00')"
        return pd.read_sql(query, con=self.cnx)


@dataclass
class DownloadHourlyOverviewAnomaly(Download):

    cnx: Any
    round_id: str
    observable_name: str

    def __post_init__(self):
        self.table = f"round_data_{self.observable_name}"

    def __call__(self, **kwargs: Dict[str, str]) -> pd.DataFrame:
        query = f"SELECT ROUND(AVG(value), 2) as mean, null as sd, ROUND(SUM(value), 2) as total,\
                DATE_FORMAT(time, '%Y-%m-%d %H:00:00') as time, round_number, day_of_production, round_id ,observable_name FROM \
                {self.table} where round_id = '{self.round_id}' and time > '{kwargs['time']}' and observable_name = '{self.observable_name}' and day_of_production >= 0 GROUP BY DATE_FORMAT(time, '%Y-%m-%d %H:00:00')"
        return pd.read_sql(query, con=self.cnx)


@dataclass
class DownloadLatest(Download):

    cnx: Any
    round_id: str
    observable_name: str

    def __post_init__(self):
        self.table = f"round_data_{self.observable_name}"

    def __call__(self, **kwargs: Dict[str, str]) -> pd.DataFrame:
        query = f"SELECT value, time, x, y, z, round_id, observable_name\
                 FROM {self.table} where round_id = '{self.round_id}' and time > '{kwargs['time']}' and observable_name = '{self.observable_name}' and day_of_production >= 0"
        return pd.read_sql(query, con=self.cnx)