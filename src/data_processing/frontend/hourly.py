from typing import List
import pandas as pd
import numpy as np
from data_processing.frontend.frontend_processing import FrontendProcessingImplementation
from data_processing.custom_exceptions.exceptions import EmptyFrontendException



class TimeInterval(FrontendProcessingImplementation):

    def download(self, col: str, group_by: str) -> pd.DataFrame:
        query = f"SELECT {col} as value, DATE_FORMAT(time, '%Y-%m-%d') AS time from {self.table_hourly} WHERE round_id='{self.round_id}' AND observable_name='{self.observable_name}' GROUP BY {group_by}(time)"
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
        
        
        
class Hour(TimeInterval):
    def __init__(self, cnx, round_id, observable_name, x_dim, y_dim, pc_details, data_events, logging):
        super().__init__(cnx, round_id, observable_name, x_dim, y_dim, pc_details, data_events, logging)
        self.table_hourly = f"event_hourly_overview_{observable_name}"
        self.event = "hour"
        
    def download(self, col: str) -> pd.DataFrame:
        query = f"SELECT {col} as value, DATE_FORMAT(time, '%Y-%m-%d %H') as time from {self.table_hourly} WHERE round_id='{self.round_id}' AND observable_name='{self.observable_name}' ORDER BY time ASC"
        df = pd.read_sql(query, con=self.cnx)
        return df
        
class HourAmbient(Hour):
    
    def get_download(self):
        return self.download(col="mean")
  
    
class HourAnomaly(Hour):
    
    def get_download(self):
        return self.download(col="total")
    
    
class Day(TimeInterval):
    def __init__(self, cnx, round_id, observable_name, x_dim, y_dim, pc_details, data_events, logging):
        super().__init__(cnx, round_id, observable_name, x_dim, y_dim, pc_details, data_events, logging)
        self.table_hourly = f"event_hourly_overview_{observable_name}"
        self.event = "day"   
    
class DayAmbient(Day):
    
    def get_download(self):
        return self.download(col="mean", group_by="DAYOFYEAR")
    
class DayAnomaly(Day):
    
    def get_download(self):
        return self.download(col="total", group_by="DAYOFYEAR")
    
    
class Week(TimeInterval):
    def __init__(self, cnx, round_id, observable_name, x_dim, y_dim, pc_details, data_events, logging):
        super().__init__(cnx, round_id, observable_name, x_dim, y_dim, pc_details, data_events, logging)
        self.table_hourly = f"event_hourly_overview_{observable_name}"
        self.event = "week"      
    
class WeekAmbient(Week):
    
    def get_download(self):
        return self.download(col="mean", group_by="WEEK")
    
class WeekAnomaly(Week):
    
    def get_download(self):
        return self.download(col="total", group_by="WEEK")
    
class Month(TimeInterval):
    def __init__(self, cnx, round_id, observable_name, x_dim, y_dim, pc_details, data_events, logging):
        super().__init__(cnx, round_id, observable_name, x_dim, y_dim, pc_details, data_events, logging)
        self.table_hourly = f"event_hourly_overview_{observable_name}"
        self.event = "week"      
    
class MonthAmbient(Month):
    
    def get_download(self):
        return self.download(col="mean", group_by="MONTH")
    
class MonthAnomaly(Month):
    
    def get_download(self):
        return self.download(col="total", group_by="MONTH")
