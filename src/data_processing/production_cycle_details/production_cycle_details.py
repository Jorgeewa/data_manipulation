from abc import ABC, abstractmethod
from typing import List, Tuple, Union, Callable, Any, Dict
from dataclasses import dataclass
import pandas as pd
from data_processing.custom_exceptions.exceptions import InvalidRound
from datetime import datetime, timedelta
import mysql.connector as con

@dataclass
class ProductionCycleDetails(ABC):

	round_id: str
	connection: Any

	@abstractmethod
	def get_day_of_production(self, first_day: datetime, current_day: datetime) -> int:
		pass

	@abstractmethod
	def get_first_day(self):
		pass

	@abstractmethod
	def get_round_number(self, start_time: str, end_time: str) -> int:
		pass

	@abstractmethod
	def get_round_time(self, time: str) -> Tuple[datetime, int, int]:
		pass




class PCDetails(ProductionCycleDetails):

	def __init__(self, cnx: con.connect, round_id: str, observable_name: str):
		self.cnx = cnx
		self.cursor = cnx.cursor()
		self.observable_name = observable_name
		self.round_id = round_id
		self.round_table = f"round_data_{observable_name}"
		self.first_day = self.get_first_day()

	def get_first_day(self) -> int:
		query = f"SELECT `from` FROM round WHERE id='{self.round_id}'"
		self.cursor.execute(query)
		day = self.cursor.fetchone()
		return day[0]

	def get_day_of_production(self, current_time: datetime) -> int:
		diff = current_time.date() - self.first_day
		return int(diff.total_seconds() / (60 * 60 * 24))

	def get_round_time(self, time: str) -> Union[Tuple[datetime, int, int], None]:
		'''
			This function returns the appropriate round end time when given a time input

			Returns the time

			Parameters
			----------
			time: the time object to check for appropriate round.

			Returns
			--------
			Returns a tuple of time object
		'''
		query = f"Select time, round_number, is_valid_round from round_counter where time<='{time}' and round_id='{self.round_id}' order by time desc limit 1"
		self.cursor.execute(query)
		return self.cursor.fetchone()

	def get_round_number(self, start_time: str, end_time: str) -> Tuple[bool, Union[int, Dict[str, List[int]]]]:
		'''
			This function returns the current round number for uploaded data

			Returns the round number

			Parameters
			----------
			time: the time of the round to check for.

			References
			-----------
			https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_sql.html

			Returns
			--------
			Returns a scalar if the round counters of the first time stamp is the same as that of the second one else it creates a class obj
		'''
		# handle where round is empty and invalid
		start = self.get_round_time(start_time)
		end = self.get_round_time(end_time)
		# Exit if round is not in range
		if start is None or end is None:
			raise InvalidRound(self.round_id, self.observable_name, f"'{self.round_id}' - Round not in range and is discarded")

		if len(start) == 0:
			raise InvalidRound(self.round_id, self.observable_name, f"'{self.round_id}' - Round didn\'t start from the charger and has been discarded")

		if start[1] == end[1]:
			if end[2] == 0:
				raise InvalidRound(self.round_id, self.observable_name, f"'{self.round_id}' - Round is not valid")
			else:
				return True, end[1]

		query = f"Select round_number, is_valid_round, time from round_counter where round_id='{ self.round_id }'  and time >='{ start[0] }' and time <='{ end[0] }'"
		round_number = pd.read_sql(query, con=self.cnx)
		d = round_number.iloc[-1, :]
		last = pd.DataFrame([{ 'round_number': d['round_number'], 'is_valid_round': d['is_valid_round'], 'time': end_time + timedelta(minutes=1)}])
		round_number = pd.concat([round_number, last]).values.tolist()
		
		return False, self.create_dict(round_number)

	def create_dict(self, df: pd.DataFrame) -> Dict[datetime, List[int]]:
		'''
			This function creates a dictionary object in class scope

			Returns None

			Parameters
			----------
			df: a data frame object with the round counter table within specific time range and the last data value appended to it.

			Notes
			----------
			The idea was to prevent querying the db for each line of code in uploaded file to obtain the round number.
			Another alternative solution would have been to use pandas and query the like I would in the db for each time stamp entry.
			I have implemented it this way because dictionary search is O(1) and even though this will lead to a bit memory inefficiency

			Returns
			--------
			Returns a None
		'''
		obj = {}

		for i in range(0, len(df) - 1):
			res = self.between_minutes( df[i][2], df[i+1][2], df[i][0], df[i][1])
			obj.update(res)
		return obj

	def between_minutes(self, start: datetime, stop: datetime, round_number: int, is_valid: int) -> Dict[datetime, List[int]]:
		'''
			This function creates all the minutes between two specified time stamps and creates a dictionary comprehension that comprises
			the the timestamps for all minutes created as key and a list value that contains the round number and whether the round is valid or not

			Returns the round number

			Parameters
			----------
			start: the start time.
			stop: the stop time.
			round number: the round number corresponding to start time.
			is valid: round validity associated with start time.

			Returns
			--------
			Returns a dictionary with key as in between mintues and value as list of round number and validity
		'''
		start_time = start.replace(second=0)
		end_time = stop.replace(second=0)

		diff = end_time - start_time
		minutes = int(diff.total_seconds() / 60)
		dictionary = { start_time + timedelta(minutes=i) : [ round_number, is_valid] for i in range( minutes + 1) }
		return dictionary

	def is_valid_observable(self, robot_id: str, observable_id) -> bool:
		query = f"select observable_id from robot_observable where observable_id='{observable_id}' and robot_id='{robot_id}' limit 1"
		self.cursor.execute(query)
		observable = self.cursor.fetchone()

		if observable is None:
			return False
		else:
			return True