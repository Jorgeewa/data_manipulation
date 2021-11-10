from abc import ABC, abstractmethod
from typing import List
from abc import ABC, abstractmethod
from typing import List, Tuple, Union, Callable, Any, Dict, Generator
from data_processing.data_processing_events.data_processing_events import DPEvents
from data_processing.data_processing_events.derived_observables import derived_observables
import pandas as pd

class DerivedObservablesProcessing(DPEvents):

	def __init__(self, cnx, round_id, observable_name):
		super().__init__(cnx, round_id, observable_name)
		self.dependencies = self.set_dependency()
	
	def set_dependency(self) -> List[str]:
		for observable in derived_observables:
			if self.observable_name == observable.name():
				return observable.get_observables()

	def check_if_ready(self) -> bool:
		'''
			Returns a boolean.

			Parameters
			----------
			None.

			Notes
			--------
			This piece of code checks if all the dependencies for a derived observable have data.
			If any of the query from the last time updated for this derived observable returns None, this returns false.

			Returns
			--------
			Boolean
		'''
		
		for observable in self.dependencies:
			res = self.check_query(observable)
			if res is None:
				return False
		return True

	def check_query(self, observable: str) -> Union[Tuple[str], None]:
		'''
			Returns a Tuple of the value or None.

			Parameters
			----------
			observable: the observable to check in the db.

			Notes
			--------
			This code runs a query for an observable obtaining data from a specified time(last time updated for the derived observable)

			Returns
			--------
			A Tuple of string or None if no data is available
		'''
		query =  f"SELECT value FROM round_data_{observable} WHERE time >'{self.last_data[2]}' AND round_id ='{self.round_id}' AND observable_name ='{observable}' LIMIT 1"
		self.cursor.execute(query)
		return self.cursor.fetchone()

	def get_range(self) -> Tuple[int, int]:
		'''
			Returns a Tuple of the start and end round numbers to process.

			Parameters
			----------
			None.

			Notes
			--------
			This code gets a range for the derived observable so we don't download too much data all at once. If there is no available round in the round counter or there is only one,
			compare the round counter with the actual round available in the round data table so tables can still be updated.

			Returns
			--------
			A Tuple of start round and end round
		'''
		observable = self.dependencies[0]
		query = f"SELECT round_number FROM round_counter WHERE time >'{self.last_data[2]}' AND round_id ='{self.round_id}' AND is_valid_round =1 ORDER BY time ASC"
		self.cursor.execute(query)
		row = self.cursor.fetchall()
		if len(row) == 0 or len(row) == 1:
			query = f"SELECT round_number FROM round_counter WHERE round_id ='{self.round_id}' AND is_valid_round =1 ORDER BY time DESC LIMIT 1"
			self.cursor.execute(query)
			row_round_counter = self.cursor.fetchone()
			
			query = f"SELECT round_number FROM { self.round_data_table } WHERE round_id ='{self.round_id}' ORDER BY time DESC LIMIT 1"
			self.cursor.execute(query)
			row_round_data = self.cursor.fetchone()
			return row_round_data[0], row_round_counter[0]
		else:
			return row[0][0], row[-1][0]

	def download_and_join_data(self, index: int) -> pd.DataFrame:
		'''
			Returns a pandas dataframe of all observables that need to merged.

			Parameters
			----------
			Index an integer of the round number to join.

			Notes
			--------
			This code downloads and joins all the observables required by this derived observable for further processing. We have agreed to do this in 
			memory because we had many issues with db processing derived observables there.

			Returns
			--------
			A pandas data frame.
		'''
		main_df = None
		for df in self.get_data(index):
			if main_df is None:
				main_df = df
			else:
				main_df = pd.merge(main_df, df, on=['time', 'x', 'y'])
		return main_df

	def get_data(self, index: int) -> Generator[pd.DataFrame, None, None]:
		'''
			Returns a generator object of pandas dataframe of an observable within a round number.

			Parameters
			----------
			Index: integer number of the round.

			Returns
			--------
			A Generator object of pandas dataframe.
		'''
		for observable in self.dependencies:
			query =  f"SELECT * FROM round_data_{observable} WHERE time >'{self.last_data[2]}' AND round_id = '{self.round_id}' AND round_number = {index} AND observable_name ='{observable}'"
			df = pd.read_sql(query, con=self.cnx)
			df = df.rename(columns = {'value': f'{observable}'})
			yield df



