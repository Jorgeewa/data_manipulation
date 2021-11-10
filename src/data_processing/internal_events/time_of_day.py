from data_processing.internal_events.events import InternalEventsGeneric
import pandas as pd
from datetime import datetime, timedelta
from data_processing.event_handlers.events import Events
from data_processing.data_processing_events.data_processing_events import TimeofDayComputation
from typing import Tuple, List, Union



class TimeofDayEvent(InternalEventsGeneric):

    def __init__(self, cnx, round_id, robot_id, observable_name, type_, xDim, yDim, download, data_events, pc_details, logging):
        super().__init__(cnx, round_id, robot_id, observable_name, type_,  xDim, yDim, download, data_events, pc_details, logging)
        self.table_over_space = f"event_time_of_day_over_space_{self.observable_name}"
        self.table_over_time = f"event_time_of_day_over_time_{self.observable_name}"
        self.params = { "roundId": round_id, "robotId": robot_id, "observableName": observable_name, "xDim": xDim, 'yDim': yDim, "event_type": Events.NEW_TIME_OF_DAY}


    def download_data(self, time_start, time_end, quadrant, midpoint: int) -> pd.DataFrame:
        return self.download(time_start=time_start, time_end=time_end, quadrant=quadrant, midpoint=midpoint)

    def process(self) -> None:
        last_time = self.data_events.get_last_time_of_day()
        final_time = self.data_events.last_data[2]
        current_quadrant = self.data_events.get_quadrant(last_time.time())
        future_quadrant = self.data_events.get_quadrant(final_time.time())
        t = TimeofDayComputation(last_time, current_quadrant)
        final_start_time, final_end_time = t.reset_quadrants(final_time, future_quadrant)

        # Trick / spaghetti code to reduce the hour in order to end while loop outside of the quadrant gotten in the round_data_table
        final_start_time = final_start_time - timedelta(hours=1)
        while last_time < final_start_time:
            start_time, last_time = t.next_quadrant_times()
            midpoint = start_time + (( last_time - start_time ) / 2 ) + timedelta(microseconds=500000)
            quadrant = t.get_current_quadrant().value
            df = self.download_data(start_time, last_time, quadrant, midpoint)

            if df.empty:
                continue
            df = self.convert_time_to_string(df)
            to_list = df.values.tolist()
            self.update_average_over_time(to_list)
            mean, sd, total = self.get_aggregations(df)
            last_series = self.get_last_line(df)
            day = last_series["day_of_production"]
            time = last_series["time"]
            params = (mean, sd, total, time, int(day), quadrant, self.round_id, self.observable_name)
            self.update_average_over_space(params)
        self.create_frontend_events(self.params)


    def update_average_over_space(self, df: Tuple[Union[float, str]]) -> None:
        # Going to have a problem with None
        query = f"INSERT INTO {self.table_over_space} (mean, sd, total, time, day, time_of_day, round_id, observable_name) \
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE mean =VALUES(mean), sd=VALUES(sd), total=VALUES(total), time=VALUES(time)"
        self.cursor.execute(query, df)
        self.cnx.commit()

    def update_average_over_time(self, df: List[str]) -> None:
        query = f"INSERT INTO {self.table_over_time} (value, time, x, y, z, round_id, day, time_of_day, observable_name)\
					VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)\
					ON DUPLICATE KEY UPDATE value=VALUES(value), time=VALUES(time)"
        self.cursor.executemany(query, df)
        self.cnx.commit()



class TimeofDayEventAmbientConditions(TimeofDayEvent):

    def get_aggregations(self, df: pd.DataFrame) -> Tuple[Union[float, None]]:
        return self.get_aggregations_ambient(df)

class TimeofDayEventAnomaly(TimeofDayEvent):

    def get_aggregations(self, df: pd.DataFrame) -> None:
        return self.get_aggregations_anomaly(df)
        