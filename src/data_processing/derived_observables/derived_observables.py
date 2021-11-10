from abc import ABC, abstractmethod
from typing import List, Tuple, Union, Callable, Any, Dict
import pandas as pd
import numpy as np
from data_processing.data_processing_events.derived_observables import derived_observables, Dobservables
from data_processing.data_processing_events.derived_observables_processing import DerivedObservablesProcessing
from data_processing.event_handlers.event_handler import trigger_event
from data_processing.event_handlers.events import Events
import time
import logging


class DerivedObservables(ABC):

	@abstractmethod
	def check_events(self) -> Dict[str, str]:
		pass

	@abstractmethod
	def process(self) -> None:
		pass

	@abstractmethod
	def upsert(self, df: pd.DataFrame) -> None:
		pass

	@abstractmethod
	def compute_specific(self) -> None:
		pass

	@abstractmethod
	def post_events(self) -> None:
		pass


class DerivedObservableImplementation(DerivedObservables):

	def __init__(
					self, 
					cnx: Any = None, 
					round_id: str = None, 
					robot_id: str = None, 
					observable_name: str = None, 
					type_: str = None, 
					xDim: int = None, 
					yDim: int = None, 
					dobp_events: DerivedObservablesProcessing = None, 
					logging: logging = None
					):
		self.cnx = cnx
		self.cursor = cnx.cursor()
		self.round_id = round_id
		self.robot_id = robot_id
		self.observable_name = observable_name
		self.logging = logging
		self.dobp = dobp_events
		self.round_data_table = f"round_data_{observable_name}"
		self.is_checked_events = False
		self.params = { "roundId": round_id, "robotId": robot_id, "observableName": observable_name, "xDim": xDim, 'yDim': yDim, "type": type_}
		self.events_params = None

	def check_events(self, **kwargs: Dict[str, str]) -> Dict[str, str]:
		new_round = self.dobp.is_new_round(kwargs['round_number'])
		new_time_of_day = self.dobp.is_new_time_of_day(kwargs['time'])
		new_day = self.dobp.is_new_day(kwargs['day'])
		new_hourly_overview = self.dobp.is_new_hourly_overview(kwargs['time'])

		return {'is_new_round': new_round, 'is_new_day': new_day, 'is_new_time_of_day': new_time_of_day, 'is_new_hourly_overview': new_hourly_overview}

	def process(self) -> None:
		global tic
		tic = time.time()
		is_ready = self.dobp.check_if_ready()
		if is_ready:
			range_ = self.dobp.get_range()
			for i in range(range_[0], range_[1] + 1):
				df = self.dobp.download_and_join_data(i)
				if df.empty:
					continue
				derived = self.compute_specific(df)
				df['value'] = derived.fillna(0)
				df['observable_name'] = self.observable_name
				df = df.rename(columns = {'round_id_x': 'round_id', 'round_number_x': 'round_number','time_x': 'time', 'day_of_production_x': 'day_of_production', 'z_x': 'z'})
				df = df[['x', 'y', 'z', 'round_number', 'value', 'time', 'round_id', 'day_of_production', 'observable_name']]
				df = df.loc[:,~df.columns.duplicated()]
				# if events haven't been checked, check it
				if self.is_checked_events is not True:
					row, col = df.shape
					final_row = df.loc[row - 1, :]
					events = {"round_number": final_row['round_number'], "day": final_row['day_of_production'], "time": final_row["time"]}
					self.events_params = self.check_events(**events)
					self.is_checked_events = True
				df['time'] = df['time'].apply(lambda x: pd.to_datetime(x).strftime("%Y-%m-%d %H:%M:%S"))
				df = df.sort_values(by=['time']).values.tolist()
				# upload data
				self.upsert(df)
			print("finished loop")
			if self.events_params is not None:
				self.post_events(**self.events_params)

	def upsert(self, df: List[str]) -> None:
		query = f"INSERT INTO {self.round_data_table} (x, y, z, round_number, value, time, round_id, day_of_production, observable_name) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) \
					ON DUPLICATE KEY UPDATE value=VALUES(value), round_number=VALUES(round_number), time=VALUES(time)"
		self.cursor.executemany(query, df)
		self.cnx.commit()
		toc = time.time()
		total_time = toc - tic	
		msg = f"Inserting data for {self.observable_name} took {total_time} secs. File last line is {df[-1][5]} and has {len(df)} lines"
		self.logging.info(msg)

	def post_events(self, **events) -> None:
		# post latest event
		trigger_event(Events.LATEST, self.params)

		# check if new round
		if events['is_new_round']:
			trigger_event(Events.NEW_ROUND, self.params)

		if events['is_new_day']:
			trigger_event(Events.NEW_NEW_DAY, self.params)

		if events['is_new_time_of_day']:
			trigger_event(Events.NEW_TIME_OF_DAY, self.params)

		if events['is_new_hourly_overview']:
			trigger_event(Events.NEW_HOURLY_OVERVIEW, self.params)


class DigestionIndex(DerivedObservableImplementation):

	def compute_specific(self, df: pd.DataFrame) -> pd.DataFrame:
		bad = df['bad_droppings']
		good = df['good_droppings']
		dig_index = (good / (bad + good)) * 100
		return dig_index


class EffectiveTemperature(DerivedObservableImplementation):

	def compute_specific(self, df: pd.DataFrame) -> pd.DataFrame:
		temp = df['temperature']
		hum = df['humidity']
		airspeed = df['airspeed']
		effective_temp = (0.94 * temp) + 0.25 * ((temp * np.arctan( 0.1515977 * np.sqrt(hum + 8.31659))) \
						+ np.arctan(temp + hum) - np.arctan(hum - 1.676331) + (0.00391838 * np.power(hum, 1.5) * np.arctan(0.023101 * hum)) - 4.686035) \
						+ 0.7 - (0.15 * (41 - temp) * (np.exp(airspeed) - np.exp(0.2)))
		return effective_temp


class Humidex(DerivedObservableImplementation):

	def compute_specific(self, df: pd.DataFrame) -> pd.DataFrame:
		temp = df['temperature']
		hum = df['humidity']
		humidex = temp + ((6.112 * (np.power(((7.5 * temp)/(237.7 + temp)), 10)) * (hum/100.0)) - 10) * (5.0/9.0)
		return humidex

class HeatStressIndex(DerivedObservableImplementation):

	def compute_specific(self, df: pd.DataFrame) -> pd.DataFrame:
		temp = df['temperature']
		hum = df['humidity']
		heat_stress_index = temp + hum
		return heat_stress_index


