from abc import ABC, abstractmethod
from typing import List, Tuple, Union, Callable, Any, Dict, TextIO
from dataclasses import dataclass
import pandas as pd
from datetime import datetime
import json
from data_processing.event_handlers.events import Events
from data_processing.custom_exceptions.exceptions import SuspiciousDataException, ObservableNotAvailable, BadFile
from data_processing.download.download import Download
from data_processing.production_cycle_details.production_cycle_details import ProductionCycleDetails
from data_processing.data_processing_events.data_processing_events import DataProcessingEvents
from data_processing.event_handlers.event_handler import trigger_event
import logging
from time import time


class DataProcessing(ABC):

	@abstractmethod
	def parse_data(self) -> None:
		pass

	@abstractmethod
	def agg_per_square_meter(self, df: pd.DataFrame) -> pd.DataFrame:
		pass

	@abstractmethod
	def read_file(self, file: TextIO) -> pd.DataFrame:
		pass

	@abstractmethod
	def check_suspicious_data(self, df: pd.DataFrame) -> None:
		pass


	@abstractmethod
	def post_time_events(self, df: pd.DataFrame) -> None:
		pass

	@abstractmethod
	def post_derived_observables(self) -> None:
		pass

	@abstractmethod
	def check_events(self) -> Dict[str, bool]:
		pass

	@abstractmethod
	def upsert(self, df: pd.DataFrame) -> None:
		pass

class DataProcessingGeneric(DataProcessing):

	def __init__(
					self, 
					cnx: Any = None, 
					file: TextIO = None, 
					round_id: str = None, 
					robot_id: str = None, 
					observable_name: str = None, 
					observable_id: str = None, 
					type_: str = None,
					download: Download = None, 
					pc_details: ProductionCycleDetails = None, 
					data_events: DataProcessingEvents = None, 
					logging: logging = None
				):
		self.cnx = cnx
		self.cursor = cnx.cursor()
		self.file = file
		self.round_id = round_id
		self.robot_id = robot_id
		self.observable_name = observable_name
		self.observable_id = observable_id
		self.download = download
		self.pc_details = pc_details
		self.data_events = data_events
		self.table = f"round_data_{observable_name}"
		self.logging = logging
		self.params = { "round_id": round_id, "robot_id": robot_id, "observable_name": observable_name, "observable_id": observable_id, "type": type_, 'cnx': cnx}


	def read_file(self) -> pd.DataFrame:
		''' 
			Reads the file. It is mostly expecting a csv file but legacy code has a json type structure.
			This should raise a key exception and can be handled with try and catch.
			Not the best code but can't think of a better way now.
		'''
		try:
			df = pd.read_csv(self.file, sep=r'\s*,\s*', header=0, engine='python')
			df['timestamp'] = [datetime.strptime(x, '"%Y-%m-%dT%H:%M:%SZ"') for x in df['timestamp']]
			# honestly can't remember why I have swapped indexes too risky to change it due to backwards compatibility
			df['coordinates'] = df['y']
			df['y'] = df['x']
			df['x'] = df['coordinates']
			df['data'] = df['value']
			df = df.drop(['coordinates', 'value'], axis=1)
			df = df.astype({"x": int, "y": int, "z": int})
			return df
		except KeyError as e:
			df = pd.read_csv(self.file, names=list('m'),sep='\t')
			df = df['m'].apply(lambda x: pd.Series(json.loads(x)))
			# honestly can't remember why I have swapped indexes
			df[['x', 'y', 'z', 'timestamp']] = [ [int(list(row[0])[0]), int(list(row[0])[1]), int(list(row[0])[2]), datetime.strptime(row[1], "%Y-%m-%dT%H:%M:%SZ")] for row in zip(df['coordinates'], df['timestamp'])]
			df = df.drop(['coordinates', 'unit'], axis=1)
			return df
		except Exception as e:
			raise BadFile(self.round_id, self.robot_id, self.observable_name, f"File is bad", self.file)

	def check_suspicious_data(self, df: pd.DataFrame) -> None:
		r, c = df.shape
		if r < 2:
			raise SuspiciousDataException(self.round_id, self.robot_id, self.observable_name, f"Suspicious data error", self.events, self.file)
		diff = df['timestamp'].iloc[-1] - df['timestamp'].iloc[0]
		diff = int(diff.total_seconds())
		values_sd = df['data'].std(axis=0)
		x_sd = df['x'].std(axis=0)
		y_sd = df['y'].std(axis=0)
		if(((values_sd < 0.01) or (x_sd == 0.00 and y_sd == 0.00)) and (diff >= 10 * 60)):
			raise SuspiciousDataException(self.round_id, self.robot_id, self.observable_name, f"Suspicious data error", self.file)

		

	def parse_data(self, df: pd.DataFrame) -> pd.DataFrame:
		if not self.pc_details.is_valid_observable(self.robot_id, self.observable_id):
			raise ObservableNotAvailable(self.round_id, self.observable_name, f"{self.observable_name} is not available for this robot")
		self.check_suspicious_data(df)
		df['round_id'] = self.round_id
		df['observable_name'] = self.observable_name
		first_row = df.iloc[0, :]['timestamp']
		last_row = df.iloc[-1, :]['timestamp']
		isRoundUnique, round_number = self.pc_details.get_round_number(first_row, last_row)
		if isRoundUnique:
			df['round_number'] = round_number
		else:
			df[['round_number', 'is_valid']] = [ round_number[time.replace(second=0)] for time in  df['timestamp']]
			# Discard invalid rounds
			df = df[df['is_valid'] == 1]

		df['day_of_production'] = self.pc_details.get_day_of_production(last_row)
		df = df.rename(columns={'timestamp': 'time', 'data': 'value'})
		df_download = self.download(list(df['time'])[0])
		df = pd.concat([df_download, df], sort=True)
		df['avg_timestamp'] = df['time'].apply(lambda x: x.value)
		df = df.sort_values(by=['time']).reset_index(drop=True)
		df = self.agg_per_square_meter(df)
		df['time'] = df['avg_timestamp'].apply(lambda x: pd.to_datetime(x).strftime("%Y-%m-%d %H:%M:%S"))
		df = df.drop(['avg_timestamp'], axis=1)
		df = df.sort_values(by=['time']).reset_index(drop=True)
		return df

	def agg_per_square_meter(self, df: pd.DataFrame) -> pd.DataFrame:
		pass

	def upsert(self, df: pd.DataFrame) -> None:
		'''
			Inserts data into the db and on duplicate key, updates

			Parameters
			----------
			df: a transformed list of files to upload.

			Returns
			--------
			None
		'''
		query = f"INSERT INTO {self.table} (x, y, z, round_number, value, time, round_id, day_of_production, observable_name) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) \
			ON DUPLICATE KEY UPDATE value=VALUES(value), round_number=VALUES(round_number), time=VALUES(time)"
		tic = time()
		self.cursor.executemany(query, df)
		print('inserting data')
		self.cnx.commit()
		self.cnx.close	
		toc = time()
		total_time = toc - tic
		msg = f'Inserting data for {self.observable_name} took {total_time} secs. File last line is {df[-1][5]} and has {len(df)} lines'
		self.logging.info(msg)

	def check_events(self, params: Dict[str, str]) -> Dict[str, bool]:
		new_round = self.data_events.is_new_round(params['round_number'])
		new_time_of_day = self.data_events.is_new_time_of_day(params['time'])
		new_day = self.data_events.is_new_day(params['day'])
		new_hourly_overview = self.data_events.is_new_hourly_overview(params['time'])

		return {'is_new_round': new_round, 'is_new_day': new_day, 'is_new_time_of_day': new_time_of_day, 'is_new_hourly_overview': new_hourly_overview}

	def post_time_events(self, **events: Dict[str, str]) -> None:

		# check if new round
		if events['is_new_round']:
			trigger_event(Events.NEW_ROUND, self.params)

			# remove invalid rounds and post new event for db update
			self.data_events.invalidate_round()

		if events['is_new_day']:
			trigger_event(Events.NEW_NEW_DAY, self.params)

		if events['is_new_time_of_day']:
			trigger_event(Events.NEW_TIME_OF_DAY, self.params)

		if events['is_new_hourly_overview']:
			trigger_event(Events.NEW_HOURLY_OVERVIEW, self.params)

	def post_derived_observables(self) -> None:

		observables = self.data_events.is_derived_observable()
		for observable in observables:
			trigger_event(Events.DERIVED_OBSERVABLE, {"round_id": self.round_id, "observable_name": observable.value, "observable_name": observable_name, "type": self._type, 'cnx': self.cnx})


class DataProcessingAmbientCondition(DataProcessingGeneric):

	def agg_per_square_meter(self, df: pd.DataFrame) -> pd.DataFrame:
		'''
			Aggregates data by square meter. Basically calculating the average of all data cordinates after round down the cordinates as int.

			Returns a transformed data frame.

			Parameters
			----------
			df: a data frame containing the data that needs to be averaged by cordinates.

			Notes
			--------
			This function is similar to groupby function in sql. It gets average value of data given cordinates and round number.

			References
			-----------
			https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.groupby.html

			Returns
			--------
			data frame
		'''
		index = { 'value': 'mean', 'time': 'first', 'avg_timestamp': 'mean','round_id': 'first', 'day_of_production': 'first', 'observable_name': 'first'}
		return df.groupby(['x', 'y', 'z', 'round_number'], as_index=False).agg(index)


class DataProcessingAnomaly(DataProcessingGeneric):
	def agg_per_square_meter(self, df):
		'''
			Aggregates data by square meter. Basically calculating the average of all data cordinates after round down the cordinates as int.

			Returns a transformed data frame.

			Parameters
			----------
			df: a data frame containing the data that needs to be averaged by cordinates.

			Notes
			--------
			This function is similar to groupby function in sql. It gets average value of data given cordinates and round number.

			References
			-----------
			https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.groupby.html

			Returns
			--------
			data frame
		'''
		index = { 'value': 'sum', 'time': 'first', 'avg_timestamp': 'mean','round_id': 'first', 'day_of_production': 'first', 'observable_name': 'first'}
		return df.groupby(['x', 'y', 'z', 'round_number'], as_index=False).agg(index)
