from abc import ABC, abstractmethod
from data_processing.download.download import Download
from data_processing.data_processing_events.data_processing_events import DataProcessingEvents
from data_processing.production_cycle_details.production_cycle_details import ProductionCycleDetails
from data_processing.event_handlers.event_handler import trigger_event
from data_processing.event_handlers.events import Events
import pandas as pd
from typing import Union, Tuple, Any, List
import datetime
import logging


class InternalEvents(ABC):

    @abstractmethod
    def get_mean(self) -> float:
        pass

    @abstractmethod
    def get_sd(self) -> float:
        pass

    @abstractmethod
    def get_total(self) -> float:
        pass

    @abstractmethod
    def download_data(self) -> float:
        pass
    
    @abstractmethod
    def get_aggregations(self) -> Tuple[Union[float, None]]:
        pass


    @abstractmethod
    def update_average_over_space(self) -> None:
        pass

    @abstractmethod
    def update_average_over_time(self) -> None:
        pass

    @abstractmethod
    def process(self) -> None:
        pass

    @abstractmethod
    def get_time(self, df: pd.DataFrame) -> datetime:
        pass

    @abstractmethod
    def create_frontend_events(self) -> None:
        pass

class InternalEventsGeneric(InternalEvents):

    def __init__(
                    self,
                    cnx: Any = None,
                    round_id: str = None,
                    robot_id: str = None,
                    observable_name: str = None,
                    type_: str = None,
                    xDim: int = None,
                    yDim: int = None,
                    download: Download = None,
                    data_events: DataProcessingEvents = None, 
                    pc_details: ProductionCycleDetails = None,
                    logging: logging = None
                ):
            self.cnx = cnx
            self.cursor = cnx.cursor()
            self.round_id = round_id
            self.robot_id = robot_id
            self.observable_name = observable_name
            self.type = type_
            self.download = download
            self.data_events = data_events
            self.table = f"round_data_{observable_name}"
            self.logging = logging
    
    def process(self) -> None:
        pass

    def get_mean(self, df: pd.DataFrame) -> float:
        return df.mean()["value"]

    def get_sd(self, df: pd.DataFrame) -> float:
        return df.std()["value"]

    def get_total(self, df: pd.DataFrame) -> float:
        return df.sum()["value"]


    def download_data(self):
        pass

    def update_average_over_space(self, df: Tuple[Union[float, str]]) -> None:
        # Going to have a problem with None
        query = f"INSERT INTO {self.table_over_space} (mean, sd, total, time, round_number, round_id, observable_name) \
        VALUES (%s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE mean =VALUES(mean), sd=VALUES(sd), total=VALUES(total), time=VALUES(time)"
        self.cursor.execute(query, df)
        self.cnx.commit()

    def update_average_over_time(self, df: List[str]) -> None:
        query = f"INSERT INTO {self.table_over_time} (value, time, x, y, z, round_id, round_number, observable_name)\
					VALUES (%s, %s, %s, %s, %s, %s, %s, %s)\
					ON DUPLICATE KEY UPDATE value=VALUES(value), time=VALUES(time)"
        self.cursor.executemany(query, df)
        self.cnx.commit()

    def get_aggregations_ambient(self, df: pd.DataFrame) -> Tuple[Union[float, None]]:
        mean = float(self.get_mean(df))
        sd = float(self.get_sd(df))
        total = None

        return mean, sd, total

    def get_aggregations_anomaly(self, df: pd.DataFrame) -> Tuple[Union[float, None]]:
        mean = None
        sd = float(self.get_sd(df))
        total = float(self.get_total(df))
        return mean, sd, total

    def get_time(self, df: pd.DataFrame) -> datetime:
        x, y = df.shape
        return df.loc[x-1, "time"]

    def get_aggregations(self) -> Tuple[Union[float, None]]:
        pass

    def create_frontend_events(self, params) -> None:
        trigger_event(Events.FRONTEND, params)
        
    def create_alert_events(self, params) -> None:
        trigger_event(Events.ALERT, params)

    def convert_time_to_string(self, df: pd.DataFrame) -> pd.DataFrame:
        df['time'] = df['time'].apply(lambda x: pd.to_datetime(x).strftime("%Y-%m-%d %H:%M:%S"))
        return df

    def get_last_line(self, df: pd.Series) -> pd.Series:
        x, y = df.shape
        return df.loc[x-1, :]