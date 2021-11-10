from abc import ABC, abstractmethod
from typing import List, Any
import pandas as pd
from data_processing.production_cycle_details.production_cycle_details import ProductionCycleDetails
from data_processing.data_processing_events.data_processing_events import DataProcessingEvents
import logging
import json



class FrontendProcessing(ABC):

    cnx: Any
    round_id: str
    observable_name: str
    table: str

    @abstractmethod
    def download(self) -> pd.DataFrame:
        pass


    @abstractmethod
    def get_json(self, df: pd.DataFrame) -> pd.DataFrame:
        pass

    @abstractmethod
    def process(self) -> None:
        pass

    @abstractmethod
    def upload(self) -> None:
        pass
    
    @abstractmethod
    def get_observable_precision(self) -> float:
        pass





class FrontendProcessingImplementation(FrontendProcessing):


    def __init__(
                    self, 
                    cnx: Any = None, 
                    round_id: str = None, 
                    observable_name: str=None, 
                    x_dim: int = None,
                    y_dim: int = None,
                    pc_details: ProductionCycleDetails = None,
                    data_events: DataProcessingEvents = None,  
                    logging: logging = None
                ):
        
        self.cnx = cnx
        self.cursor = cnx.cursor()
        self.round_id = round_id
        self.observable_name = observable_name
        self.x_dim = x_dim + 1
        self.y_dim = y_dim + 1
        self.table = f"frontend_data"
        self.table_time = f""
        self.pc_details = pc_details
        self.data_events = data_events
        self.logging = logging

    def download(self) -> pd.DataFrame:
        pass


    def get_json(self, df: pd.DataFrame) -> pd.DataFrame:
        pass

    def process(self) -> None:
        pass

    def upload(self, data: json = None, event_type: str = None, type_of_plot: str = None) -> None:
        
        query = f"INSERT INTO {self.table} (data, event_type, type_of_plot, round_id, observable_name) VALUES (%s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE data=VALUES(data)"
        row = (data, event_type, type_of_plot, self.round_id, self.observable_name)
        self.cursor.execute(query, row)
        self.cnx.commit()
        
        
    def get_observable_precision(self) -> float:
        query = f"SELECT observable_precision FROM observable WHERE code_name ='{self.observable_name}"
        self.cursor.execute(query)
        precision = self.cursor.fetchone()
        return precision[0]
    
    def get_json(self, df: List[str], date: List[str]) -> pd.DataFrame:
        return json.dumps({'data': df, 'date': date}).replace('NaN', 'null')