from data_processing.internal_events.events import InternalEventsGeneric
import pandas as pd
from data_processing.event_handlers.events import Events
from typing import Tuple, List, Union



class NewDayEvent(InternalEventsGeneric):

    def __init__(self, cnx, round_id, robot_id, observable_name, type_, xDim, yDim, download, data_events, pc_details, logging):
        super().__init__(cnx, round_id, robot_id, observable_name, type_,  xDim, yDim, download, data_events, pc_details, logging)
        self.table_over_space = f"event_new_day_over_space_{self.observable_name}"
        self.table_over_time = f"event_new_day_over_time_{self.observable_name}"
        self.params = { "roundId": round_id, "robotId": robot_id, "observableName": observable_name, "xDim": xDim, 'yDim': yDim, "event_type": Events.NEW_NEW_DAY}


    def download_data(self, day: int) -> pd.DataFrame:
        return self.download(day=day)

    def process(self) -> None:
        old_day = max(self.data_events.get_last_updated_day_number(), 0)
        current_day = max(self.data_events.last_data[1], 0)

        for day in range(old_day, current_day):
            df = self.download_data(day)

            if df.empty:
                continue
            df = self.convert_time_to_string(df)
            to_list = df.values.tolist()
            self.update_average_over_time(to_list)
            mean, sd, total = self.get_aggregations(df)
            time = self.get_time(df)
            params = (mean, sd, total, time, day, self.round_id, self.observable_name)
            self.update_average_over_space(params)
        self.create_frontend_events(self.params)

    def update_average_over_space(self, df: Tuple[Union[float, str]]) -> None:
        # Going to have a problem with None
        query = f"INSERT INTO {self.table_over_space} (mean, sd, total, time, day, round_id, observable_name) \
        VALUES (%s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE mean =VALUES(mean), sd=VALUES(sd), total=VALUES(total), time=VALUES(time)"
        self.cursor.execute(query, df)
        self.cnx.commit()

    def update_average_over_time(self, df: List[str]) -> None:
        query = f"INSERT INTO {self.table_over_time} (value, time, x, y, z, round_id, day, observable_name)\
					VALUES (%s, %s, %s, %s, %s, %s, %s, %s)\
					ON DUPLICATE KEY UPDATE value=VALUES(value), time=VALUES(time)"
        self.cursor.executemany(query, df)
        self.cnx.commit()


class NewDayEventAmbientConditions(NewDayEvent):

    def get_aggregations(self, df: pd.DataFrame) -> Tuple[Union[float, None]]:
        return self.get_aggregations_ambient(df)

class NewDayEventAnomaly(NewDayEvent):

    def get_aggregations(self, df: pd.DataFrame) -> None:
        return self.get_aggregations_anomaly(df)
        