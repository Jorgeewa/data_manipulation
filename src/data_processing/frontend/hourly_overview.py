from typing import List
import pandas as pd
import numpy as np
from data_processing.frontend.frontend_processing import FrontendProcessingImplementation
from data_processing.custom_exceptions.exceptions import EmptyFrontendException



class HourlyOverview(FrontendProcessingImplementation):
    
    def __init__(self, cnx, round_id, observable_name, x_dim, y_dim, pc_details, data_events, logging):
        super().__init__(cnx, round_id, observable_name, x_dim, y_dim, pc_details, data_events, logging)
        self.table_hourly = f"event_hourly_overview_{observable_name}"
        self.event = "hourly_overview"


    def download(self, col: str) -> pd.DataFrame:
        query = f"SELECT {col} as value, time, day, CAST(DATE_FORMAT(time, '%k')AS UNSIGNED) AS hour from {self.table_hourly} WHERE round_id='{self.round_id}' AND observable_name='{self.observable_name}' GROUP BY DATE_FORMAT(time, '%Y-%m-%d %H:00:00')"
        df = pd.read_sql(query, con=self.cnx)
        return df      
                
    def process(self) -> None:
        dim = self.data_events.last_data[1] + 1
        df = self.get_download()
        if df.empty:
            raise EmptyFrontendException(self.round_id, self.observable_name, f"Frontend graph for {self.event} has failed because there is not data for round_id={self.round_id}, observable name = {self.observable_name}")
        hour = df['hour'].values
        days_data = df['day'].values
        value = df['value'].values
        day = np.chararray(dim, itemsize=20, unicode=True)
        
        # not ok messing with frontend things here. Review this code
        for x in range(dim):
            day[x] = 'Day ' + str(x)
        hour_matrix = np.full([24, dim], np.nan, dtype=np.float64)
        hour_matrix[hour, days_data] = value
        with np.errstate(invalid='ignore'):
            hour_matrix[hour_matrix < 0] = 0
        hour_matrix = hour_matrix.tolist()
        day = day.tolist()
        data = self.get_json(hour_matrix, day)
        
        # upload data
        self.upload(data=data, event_type=self.event, type_of_plot="heatmap")
        
        
        
class HourlyOverviewAmbient(HourlyOverview):
    
    def get_download(self):
        return self.download(col="mean")


class HourlyOverviewAnomaly(HourlyOverview):
    
    def get_download(self):
        return self.download(col="total")