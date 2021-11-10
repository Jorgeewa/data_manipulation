from data_processing.internal_events.events import InternalEventsGeneric
import pandas as pd
from datetime import datetime, date
from data_processing.event_handlers.events import Events
from typing import Tuple, List, Union



class LatestEvent(InternalEventsGeneric):

    def __init__(self, cnx, round_id, robot_id, observable_name, type_, xDim, yDim, download, data_events, pc_details, logging):
        super().__init__(cnx, round_id, robot_id, observable_name, type_,  xDim, yDim, download, data_events, pc_details, logging)
        self.table_over_space = f"event_latest_over_space_{self.observable_name}"
        self.table_over_time = f"event_latest_over_time_{self.observable_name}"
        self.params = { "roundId": round_id, "robotId": robot_id, "observableName": observable_name, "xDim": xDim, 'yDim': yDim, "event_type": Events.LATEST}


    def download_data(self, time: datetime) -> pd.DataFrame:
        return self.download(time=time)
        #return self.download({"time": time})

    def process(self) -> None:
        old_time = self.data_events.get_last_updated_latest()
        df = self.download_data(old_time)
        if df.empty:
            return
        df = self.convert_time_to_string(df)
        to_list = df.values.tolist()
        self.update_average_over_time(to_list)
        mean, sd, total = self.get_aggregations(df)
        latest_time = self.get_time(df)
        params = (mean, sd, total, latest_time, str(old_time), self.round_id, self.observable_name)
        self.update_average_over_space(params)

        self.create_frontend_events(self.params)


    def update_average_over_space(self, df: Tuple[Union[float, str]]) -> None:
        # I have not set a restriction on the db so I can't use upsert here
        if df[4] == date(1970, 1, 1):
            query = f"INSERT INTO {self.table_over_space} (mean, sd, total, latest_time, old_time, round_id, observable_name) \
                    VALUES (%s, %s, %s, %s, %s, %s)"
        else:
            query = f"UPDATE {self.table_over_space} set mean=%s, sd=%s, total=%s, latest_time=%s, old_time=%s WHERE round_id=%s AND observable_name=%s" 
        self.cursor.execute(query, df)
        self.cnx.commit()


    def update_average_over_time(self, df: List[str]) -> None:
        # potential discrepancy with other code where I was using round and day > 2 possibly because robot sent shit data before that
        query = f"INSERT INTO {self.table_over_time} (value, time, x, y, z, round_id, observable_name)\
					VALUES (%s, %s, %s, %s, %s, %s, %s)\
					ON DUPLICATE KEY UPDATE value=VALUES(value), time=VALUES(time)"
        self.cursor.executemany(query, df)
        self.cnx.commit()


class LatestEventAmbientConditions(LatestEvent):

    def get_aggregations(self, df: pd.DataFrame) -> Tuple[Union[float, None]]:
        return self.get_aggregations_ambient(df)

class LatestEventAnomaly(LatestEvent):

    def get_aggregations(self, df: pd.DataFrame) -> None:
        return self.get_aggregations_anomaly(df)
        