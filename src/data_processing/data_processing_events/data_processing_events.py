from abc import ABC, abstractmethod
from typing import List, Tuple, Union, Callable, Any, Dict
from dataclasses import dataclass
import pandas as pd
from datetime import datetime, timedelta, date
from enum import Enum, auto
from data_processing.event_handlers.event_handler import trigger_event
from data_processing.data_processing_events.derived_observables import derived_observables


class TimeofDayPeriod(Enum):
	MORNING_START = datetime.strptime('08:00:00', '%H:%M:%S').time()
	MORNING_END = datetime.strptime('11:59:59', '%H:%M:%S').time()
	AFTERNOON_START = datetime.strptime('12:00:00', '%H:%M:%S').time()
	AFTERNOON_END = datetime.strptime('15:59:59', '%H:%M:%S').time()
	EVENING_START = datetime.strptime('16:00:00', '%H:%M:%S').time()
	EVENING_END = datetime.strptime('19:59:59', '%H:%M:%S').time()
	NIGHT_START = datetime.strptime('20:00:00', '%H:%M:%S').time()
	NIGHT_END = datetime.strptime('07:59:59', '%H:%M:%S').time()

	def __le__(self, other):
		return self.value <= other

	def __ge__(self, other):
		return self.value >= other

	def __sub__(self, other):
		return self.value - other.value


class TimeofDay(Enum):
	MORNING = auto()
	AFTERNOON = auto()
	EVENING = auto()
	NIGHT = auto()



@dataclass
class DataProcessingEvents(ABC):

	round_id: str
	connection: Any

	@abstractmethod
	def is_new_round(self, round_number: int) -> bool:
		pass

	@abstractmethod
	def is_new_time_of_day(self, table: str) -> bool:
		pass

	@abstractmethod
	def is_new_day(self, table: str) -> bool:
		pass

	@abstractmethod
	def is_new_hourly_overview(self, table: str) -> bool:
		pass

	@abstractmethod
	def is_new_hourly(self, table: str) -> bool:
		pass

	@abstractmethod
	def invalidate_round(self, round_number: int) -> None:
		pass

	@abstractmethod
	def is_derived_observable(self) -> bool:
		pass		




class DPEvents(DataProcessingEvents):

	def __init__(self, cnx: Any, round_id: str, observable_name: str):
		self.cnx = cnx
		self.cursor = self.cnx.cursor()
		self.observable_name = observable_name
		self.round_id = round_id
		self.round_table = f"round_data_{observable_name}"
		self.last_data = self.get_last_main_table_row()

	def get_last_main_table_row(self):
		query = f"SELECT round_number, day_of_production, time FROM {self.round_table} WHERE round_id='{self.round_id}' AND observable_name='{self.observable_name}' ORDER BY time DESC LIMIT 1"
		self.cursor.execute(query)
		row = self.cursor.fetchone()
		if row is not None:
			return row
		else:
			return (0, 0, date(1970, 0, 0))

	def is_new_round(self, round_number: int) -> bool:
		return self.last_data[0] != round_number

	def get_last_time_of_day(self) -> Union[datetime, None]:
		query = f"SELECT time FROM event_time_of_day_over_space_{self.observable_name} WHERE round_id='{self.round_id}' AND observable_name='{self.observable_name}' ORDER BY time DESC LIMIT 1"
		self.cursor.execute(query)
		row = self.cursor.fetchone()
		if row is not None:
			return row[0]
		else:
			return None

	def get_last_hourly_overview(self) -> Union[datetime, None]:
		query = f"SELECT time FROM event_hourly_overview_{self.observable_name} WHERE round_id='{self.round_id}' AND observable_name='{self.observable_name}' ORDER BY time DESC LIMIT 1"
		self.cursor.execute(query)
		row = self.cursor.fetchone()
		if row is not None:
			return row[0]
		else:
			return None

	def is_new_time_of_day(self, time: datetime) -> bool:
		current_quadrant = self.get_quadrant(time.time())
		last_time = self.get_last_time_of_day()
		last_quadrant = self.get_quadrant(last_time.time())
		if(last_time is None):
			return True
		diff = time.date() - last_time.date()
		number_of_days = int(diff.total_seconds() / (60 * 60 * 24))
		quadrant_change = abs(last_quadrant - current_quadrant) >= 1
		# If day is greater than 1 then there is a change for sure, however this if condition won't catch when there is a one day difference and the robot wasn't working.
		if(number_of_days > 1 or quadrant_change):
			return True
		else:
			return False

	def get_quadrant(self, time:datetime.time) -> TimeofDay:
		if(TimeofDayPeriod.MORNING_START <= time and TimeofDayPeriod.MORNING_END >= time):
			return TimeofDay.MORNING
		elif(TimeofDayPeriod.AFTERNOON_START <= time and TimeofDayPeriod.AFTERNOON_END >= time):
			return TimeofDay.AFTERNOON
		elif(TimeofDayPeriod.EVENING_START <= time and TimeofDayPeriod.EVENING_END >= time):
			return TimeofDay.EVENING
		else:
			return TimeofDay.NIGHT

	def is_new_day(self, day: int) -> bool:
		return self.last_data[1] != day

	def is_new_hourly_overview(self, time: datetime) -> bool:
		last_time = self.get_last_hourly_overview()
		if(last_time is None):
			return True
		else:
			total = self.last_data[2] - time
			return (total.total_seconds() // 60) >= 1

	def is_new_hourly(self, table: str) -> bool:
		pass

	def invalidate_round(self, round_number: int) -> None:
		'''
			Returns None.

			Parameters
			----------
			round_number: the round number.

			Notes
			--------
			This piece of code deletes data from the db if there are cases of spurious data and updates the alerts table without sending any alert.
			Three possible scenarios to capture here
			1) When the good round happens before all the bad ones.
			2) When the good round happens after all the bad ones.
			3) When the good round is in between the bad ones.

			Returns
			--------
			None
		'''

		# check if there is bad round
		round_number -= 1
		query = f"Select count(*) from round_counter where round_id = '{self.round_id}' and round_number={round_number}"
		self.cursor.execute(query)
		number = self.cursor.fetchone()
		# Check if none
		if number is None:
			return
		
		# If there is no bad round return
		if number[0] == 1 or number[0] == 0:
			return
		else:
			# get valid time
			query = f"Select time, round_number from round_counter where round_id='{self.round_id}' and round_number={round_number} and is_valid_round = 1 order by time desc limit 1"
			self.cursor.execute(query)
			valid_time, round_number = self.cursor.fetchone()
			

			# get other invalid time
			query = f"Select time, round_number from round_counter where round_id='{self.round_id}' and round_number={round_number} and is_valid_round=0 order by time asc"
			self.cursor.execute(query)
			invalid_times = self.cursor.fetchall()
			

			for invalid_time in invalid_times:
				time, round_number = invalid_time

				# condition one (where all the invalid times are greater than the good one)
				if time > valid_time:
					# This should delete all the invalid times so break out from the loop
					query = f"delete from {self.table} where time>='{time}' and round_number={round_number} and round_id='{self.round_id}'"
					self.cursor.execute(query)
					self.cnx.commit()
					trigger_event('invalid_round', {'round_id': self.round_id, 'round_number': round_number, 'time': time})
					break
				else:
					# This should delete all the invalid times till the valid one
					query = f"delete from {self.table} where time>='{time}' and time <'{valid_time}' and round_number={round_number} and round_id='{self.round_id}'"
					self.cursor.execute(query)
					self.cnx.commit()	
					trigger_event('invalid_round', {'round_id': self.round_id, 'round_number': round_number, 'time': time})


	def is_derived_observable(self) -> Dict[str, str]:

		observables = []
		for observable in derived_observables:
			if self.observable_name in observable.get_observables():
				observables.append(observable.name())

		return observables