from typing import List
import pandas as pd
import numpy as np
from data_processing.frontend.frontend_processing import FrontendProcessingImplementation
from data_processing.custom_exceptions.exceptions import EmptyFrontendException, WrongCordinatesException



class Heatmap(FrontendProcessingImplementation):


    def download(self, col: str) -> pd.DataFrame:
        query = f"SELECT value, time, x, y, z, {col} FROM {self.table_time} WHERE round_id ='{self.round_id}' \
                AND observable_name = '{self.observable_name}' AND x >= 0 and y >= 0 ORDER BY TIME ASC"
        df = pd.read_sql(query, con=self.cnx)
        return df
                
                
    def get_time_stamps(self, col: str) -> pd.DataFrame:
        query = f"SELECT DISTINCT {col}, time FROM {self.table_space} WHERE round_id='{self.round_id}' and observable_name='{self.observable_name}' ORDER BY TIME ASC"
        df = pd.read_sql(query, con=self.cnx)
        return df
    
    
    def check_cordinates(self, x, y) -> None:
        is_x = np.any(x>self.x_dim)
        is_y = np.any(y>self.y_dim)
        
        if is_x or is_y:
            raise WrongCordinatesException(self.round_id, x, y, "Wrong cordinates exception")
        
        
        
        
        
class HeatmapLatest(Heatmap):
    
    def __init__(self, cnx, round_id, observable_name, x_dim, y_dim, pc_details, data_events, logging):
        super().__init__(cnx, round_id, observable_name, x_dim, y_dim, pc_details, data_events, logging)
        self.table_time = f"event_latest_over_time_{observable_name}"
        self.table_space = f"event_latest_over_space_{observable_name}"
        
        
    def process(self) -> None:
        df = self.download(col="observable_name")
        
        if df.empty:
            raise EmptyFrontendException(self.round_id, self.observable_name, f"There is no latest event data and frontend event for latest has failed round_id={self.round_id}, observable name = {self.observable_name}")
        latest = {}
        
        x = df.loc[:, 'x'].values
        y = df.loc[:, 'y'].values
        v = df.loc[:, 'value'].values
        t = df.loc[:, 'time'].values
        self.check_cordinates(x, y)
        out = np.full([self.x_dim, self.y_dim], np.nan, dtype=np.float64)
        out[x, y] = v
        with np.errstate(invalid='ignore'):
            out[out < 0] = 0
            out = np.round(out, decimals=3)
        ts = pd.to_datetime(str(t[-1])) 
        timestamp = ts.strftime("%d-%m-%y")
        day_of_production = [str(self.pc_details.get_day_of_production(ts))]
        latest[timestamp] = out.tolist()
  

        data = self.get_json(latest, day_of_production)
        
        # upload data
        self.upload(data=data, event_type="latest", type_of_plot="heatmap")
        
        
class HeatmapRound(Heatmap):
    
    def __init__(self, cnx, round_id, observable_name, x_dim, y_dim, pc_details, data_events, logging):
        super().__init__(cnx, round_id, observable_name, x_dim, y_dim, pc_details, data_events, logging)
        self.table_time = f"event_round_over_time_{observable_name}"
        self.table_space = f"event_round_over_space_{observable_name}"
        
    def process(self) -> None:
        last_dim = self.data_events.get_last_updated_round()
        df = self.download(col="round_number")
        
        if df.empty:
            raise EmptyFrontendException(self.round_id, self.observable_name, f"There is no round data and frontend event for round has failed round_id={self.round_id}, observable name = {self.observable_name}")
        timestamp = self.get_time_stamps(col="round_number")
        ts = timestamp['time'].values
        round_number = timestamp['round_number'].values

        x = df['x'].values
        y = df['y'].values
        v = df['value'].values
        r = df['round_number'].values
        self.check_cordinates(x, y)
        out = np.full([last_dim + 1, self.x_dim, self.y_dim], np.nan, dtype=np.float64)
        out[r, x, y] = v
        with np.errstate(invalid='ignore'):
            out[out < 0] = 0
            out = np.round(out, decimals=3)
        round, day_of_production = self.dict_(out.tolist(), ts, round_number)
        data = self.get_json(round, day_of_production)
        # upload data
        self.upload(data=data, event_type="round", type_of_plot="heatmap")
        
        
        
    # This version only shows available data
    def dict_(self, data, timestamp, r):
        updated_data = {}
        day_of_production = []
        for (r, timestamp) in zip(r, timestamp):
            ts = pd.to_datetime(str(timestamp)) 
            day_of_production.append(int(self.pc_details.get_day_of_production(ts)))
            date = ts.strftime("%d-%m %H:%M")
            '''
                Try and catch here because I am getting the round numbers from temperature and sometimes especially for anomalies they might not be equal. I am doing this so that the
                Time stamps are consisitent. When we move to the round counter this can be removed.
            '''
            try:
                updated_data[f"Round {r} ({date})"] = data[r]
            except:
                continue
        return updated_data, day_of_production
        
        
        
class HeatmapNewDay(Heatmap):
    
    def __init__(self, cnx, round_id, observable_name, x_dim, y_dim, pc_details, data_events, logging):
        super().__init__(cnx, round_id, observable_name, x_dim, y_dim, pc_details, data_events, logging)
        self.table_time = f"event_new_day_over_time_{observable_name}"
        self.table_space = f"event_new_day_over_space_{observable_name}"
        
    
    def process(self):
        last_dim = self.data_events.get_last_updated_day_number()
        df = self.download(col="day")
        if df.empty:
            raise EmptyFrontendException(self.round_id, self.observable_name, f"There is no new day data and frontend event for new day has failed round_id={self.round_id}, observable name = {self.observable_name}")

        timestamp = self.get_time_stamps(col="day")
        ts = timestamp['time'].values
        day_unique = timestamp['day'].values
        x = df['x'].values
        y = df['y'].values
        v = df['value'].values
        d = df['day'].values
        t = df['time'].values
        self.check_cordinates(x, y)
        out = np.full([last_dim + 1, self.x_dim, self.y_dim], np.nan, dtype=np.float64)
        out[d, x, y] = v
        with np.errstate(invalid='ignore'):
            out[out < 0] = 0
            out = np.round(out, decimals=3)
        days = self.dict_(out.tolist(), ts, day_unique)
        
        data = self.get_json(days, day_unique.tolist())
        
        # upload data
        self.upload(data=data, event_type="new_day", type_of_plot="heatmap")

    # This version only shows available data
    def dict_(self, data, timestamp, days):
        updated_data = {}
        for (day, timestamp) in zip(days, timestamp):
            ts = pd.to_datetime(str(timestamp)) 
            date = ts.strftime("%d-%m-%y")
            updated_data[f"Day {day} ({date})"] = data[day]
        return updated_data
        
        
        
class HeatmapTimeofDay(Heatmap):
    
    def __init__(self, cnx, round_id, observable_name, x_dim, y_dim, pc_details, data_events, logging):
        super().__init__(cnx, round_id, observable_name, x_dim, y_dim, pc_details, data_events, logging)
        self.table_time = f"event_time_of_day_over_time_{observable_name}"
        self.table_space = f"event_time_of_day_over_space_{observable_name}"
        
        
    def process(self):
        df = self.download(col="day")
        if df.empty:
            raise EmptyFrontendException(self.round_id, self.observable_name, f"There is no time of day data and frontend event for time of day has failed round_id={self.round_id}, observable name = {self.observable_name}")
        timestamp = self.get_time_stamps(col="time_of_day")
        timestamp = timestamp.loc[:, "time"].values
        df = df.set_index('time')
        days = {}
        day_of_production = []
        default = np.full([self.x_dim, self.y_dim], np.nan, dtype=np.float64)
        for time in timestamp:
            val = df.loc[df.index == time, :]
            x = val[ 'x'].values
            y = val['y'].values
            v = val['value'].values
            t = val.index.values
            d = val['day'].values
            self.check_cordinates(x, y)
            out = default.copy()
            out[x, y] = v
            with np.errstate(invalid='ignore'):
                out[out == 0] = np.nan
                out[out < 0] = 0
                out = np.round(out, decimals=3)
            try:
                ts = pd.to_datetime(str(t[0])) 
                day_of_production.append(int(d[-1]))
                date = ts.strftime("%d-%m %H:%M")
                days[f'{date}'] = out.tolist()
            except:
                continue
        data = self.get_json(days, day_of_production)
        
        # upload data
        self.upload(data=data, event_type="time_of_day", type_of_plot="heatmap")
        