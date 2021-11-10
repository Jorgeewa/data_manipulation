from typing import List
import pandas as pd
import numpy as np
from data_processing.frontend.frontend_processing import FrontendProcessingImplementation
from data_processing.custom_exceptions.exceptions import EmptyFrontendException



class Graph(FrontendProcessingImplementation):


    def download(self, time: str, col: str, date_format: str) -> pd.DataFrame:
        query = f"SELECT DATE_FORMAT({time}, '{date_format}') as time, {col} as value from {self.table_space} WHERE round_id='{self.round_id}' AND observable_name='{self.observable_name}' GROUP BY {time}"
        df = pd.read_sql(query, con=self.cnx)
        return df      
                
    def process(self) -> None:
        
        df = self.get_download()
        if df.empty:
            raise EmptyFrontendException(self.round_id, self.observable_name, f"Frontend graph for {self.event} has failed because there is not data for round_id={self.round_id}, observable name = {self.observable_name}")
        data = df.loc[:, 'value'].values.tolist()
        timestamp = df.loc[:, 'time'].values.tolist()
        data = self.get_json(data, timestamp)
        # upload data
        self.upload(data=data, event_type=self.event, type_of_plot="graph")
        
        
        
class GraphLatest(Graph):
    
    def __init__(self, cnx, round_id, observable_name, x_dim, y_dim, pc_details, data_events, logging):
        super().__init__(cnx, round_id, observable_name, x_dim, y_dim, pc_details, data_events, logging)
        self.table_time = f"event_latest_over_time_{observable_name}"
        self.table_space = f"event_latest_over_space_{observable_name}"
        self.event = "latest"
        
        
    def get_download(self) -> pd.DataFrame:
        pass
    
class GraphLatestAmbientCondition(GraphLatest):
            
    def get_download(self) -> pd.DataFrame:
        time = "latest_time"
        col = "mean"
        date_format = '%Y-%m-%d %H:00:00'
        return self.download(col, time, date_format)
    
    
class GraphLatestAnomaly(Graph):
    
    def get_download(self) -> pd.DataFrame:
        time = "latest_time"
        col = "total"
        date_format = '%Y-%m-%d %H:00:00'
        return self.download(col, time, date_format)
        
        
class GraphRound(Graph):
    
    def __init__(self, cnx, round_id, observable_name, x_dim, y_dim, pc_details, data_events, logging):
        super().__init__(cnx, round_id, observable_name, x_dim, y_dim, pc_details, data_events, logging)
        self.table_time = f"event_round_over_time_{observable_name}"
        self.table_space = f"event_round_over_space_{observable_name}"
        self.event = "round"
        
    def get_download(self) -> pd.DataFrame:
        pass
    
    
class GraphRoundAmbient(GraphRound):
        
    def get_download(self) -> pd.DataFrame:
        time = "time"
        col = "mean"
        date_format = "%Y-%m-%d %H:%i:00"
        return self.download(time, col, date_format)
    
    
class GraphRoundAnomaly(Graph):
    
    def get_download(self) -> pd.DataFrame:
        time = "time"
        col = "total"
        date_format = "%Y-%m-%d %H:%i:00"
        return self.download(time, col, date_format)
        
        
        
        
class GraphNewDay(Graph):
    
    def __init__(self, cnx, round_id, observable_name, x_dim, y_dim, pc_details, data_events, logging):
        super().__init__(cnx, round_id, observable_name, x_dim, y_dim, pc_details, data_events, logging)
        self.table_time = f"event_new_day_over_time_{observable_name}"
        self.table_space = f"event_new_day_over_space_{observable_name}"
        self.event = "new_day"
        
    
    def get_download(self) -> pd.DataFrame:
        pass
    
class GraphNewDayAmbient(GraphNewDay):
        
    def get_download(self) -> pd.DataFrame:
        time = "time"
        col = "mean"
        date_format = "%Y-%m-%d 00:00:00"
        return self.download(time, col, date_format)
    
    
class GraphNewDayAnomaly(GraphNewDay):
    
    def get_download(self) -> pd.DataFrame:
        time = "time"
        col = "total"
        date_format = "%Y-%m-%d 00:00:00"
        return self.download(time, col, date_format)
        
        
        
        
class GraphTimeofDay(GraphNewDay):
    
    def __init__(self, cnx, round_id, observable_name, x_dim, y_dim, pc_details, data_events, logging):
        super().__init__(cnx, round_id, observable_name, x_dim, y_dim, pc_details, data_events, logging)
        self.table_time = f"event_time_of_day_over_time_{observable_name}"
        self.table_space = f"event_time_of_day_over_space_{observable_name}"
        self.event = "time_of_day"
        
        
    def get_download(self) -> pd.DataFrame:
        pass
    
    
class GraphTimeofDayAmbient(GraphNewDay):
     
    def get_download(self) -> pd.DataFrame:
        time = "time"
        col = "mean"
        date_format = "%Y-%m-%d %H:%i:00"
        return self.download(col, time, date_format)
    
    
    
    
class GraphTimeofDayAnomaly(Graph):
    
    def get_download(self) -> pd.DataFrame:
        time = "time"
        col = "total"
        date_format = "%Y-%m-%d %H:%i:00"
        return self.download(col, time, date_format)
        
        
        
        
        
        