from data_processing.internal_events.events import InternalEventsGeneric
import pandas as pd
from data_processing.event_handlers.events import Events
from typing import Tuple, List, Union



class RoundEvent(InternalEventsGeneric):

	def __init__(self, cnx, round_id, robot_id, observable_name, type_, xDim, yDim, download, data_events, pc_details, logging):
		super().__init__(cnx, round_id, robot_id, observable_name, type_,  xDim, yDim, download, data_events, pc_details, logging)
		self.table_over_space = f"event_round_over_space_{self.observable_name}"
		self.table_over_time = f"event_round_over_time_{self.observable_name}"
		self.params = { "roundId": round_id, "robotId": robot_id, "observableName": observable_name, "xDim": xDim, 'yDim': yDim, "event_type": Events.NEW_ROUND}


	def download_data(self, round_number: str) -> pd.DataFrame:
		return self.download(round_number=round_number)

	def process(self) -> None:
		current_round = self.data_events.get_last_updated_round()
		old_round = self.data_events.last_data[0]
		for round_number in range(current_round, old_round):
			df = self.download_data(round_number)

			if df.empty:
				continue
			df = self.convert_time_to_string(df)
			to_list = df.values.tolist()
			self.update_average_over_time(to_list)
			mean, sd, total = self.get_aggregations(df)
			time = self.get_time(df, round_number)
			params = (mean, sd, total, time, round_number, self.round_id, self.observable_name)
			self.update_average_over_space(params)
		self.create_frontend_events(self.params)

	def get_time(self, df, round_number) -> str:
		query = f"Select time from round_counter where round_number='{round_number}' and round_id='{self.round_id}' and is_valid_round=1"
		self.cursor.execute(query)
		time = self.cursor.fetchone()[0]

		# Test this bit of code
		if time is None:
			x, y = df.shape
			first_time = df.loc[0, "time"]
			last_time = df.loc[x-1, "time"]
			return start_time + (( end_time - start_time ) / 2 )
		else:
			return time

	def update_average_over_space(self, df: Tuple[Union[float, str]]) -> None:
		# Going to have a problem with None
		query = f"INSERT INTO {self.table_over_space} (mean, sd, total, time, round_number, round_id, observable_name) \
		VALUES (%s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE mean =VALUES(mean), sd=VALUES(sd), total=VALUES(total), time=VALUES(time)"
		self.cursor.execute(query, df)
		self.cnx.commit()

	def update_average_over_time(self, df: List[str]) -> None:
		query = f"INSERT INTO {self.table_over_time} (value, time, x, y, z, round_id, round_number, observable_name)\
					VALUES (%s, %s, %s, %s, %s, %s, %s, %s)\
					ON DUPLICATE KEY UPDATE value=VALUES(value), time=VALUES(time)"
		self.cursor.executemany(query, df)
		self.cnx.commit()

class RoundEventAmbientConditions(RoundEvent):

	def get_aggregations(self, df: pd.DataFrame) -> Tuple[Union[float, None]]:
		return self.get_aggregations_ambient(df)

class RoundEventAnomaly(RoundEvent):

	def get_aggregations(self, df: pd.DataFrame) -> None:
		return self.get_aggregations_anomaly(df)
		