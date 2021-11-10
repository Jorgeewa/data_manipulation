from data_processing.internal_events.events import InternalEventsGeneric
import pandas as pd
from data_processing.event_handlers.events import Events
from typing import Tuple, List, Union


class HourlyOverviewEvent(InternalEventsGeneric):

    def __init__(self, cnx, round_id, robot_id, observable_name, type_, xDim, yDim, download, data_events, pc_details, logging):
        super().__init__(cnx, round_id, robot_id, observable_name, type_,  xDim, yDim, download, data_events, pc_details, logging)
        self.table_over_space = f"event_hourly_overview_{self.observable_name}"
        self.params = { "roundId": round_id, "robotId": robot_id, "observableName": observable_name, "xDim": xDim, 'yDim': yDim, "event_type": Events.NEW_HOURLY_OVERVIEW}


    def download_data(self, time: int) -> pd.DataFrame:
        return self.download(time=time)

    def process(self) -> None:
        time = self.data_events.get_last_hourly_overview()
        df = self.download_data(time)
        if df.empty:
            return
        print(df)
        df = self.convert_time_to_string(df)
        to_list = df.values.tolist()
        self.update_average_over_space(to_list)
        self.create_frontend_events(self.params)

    def update_average_over_space(self, df: Tuple[Union[float, str]]) -> None:
        # Going to have a problem with None
        query = f"INSERT INTO {self.table_over_space} (mean, sd, total, time, round_number, day, round_id, observable_name) \
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE mean =VALUES(mean), sd=VALUES(sd), total=VALUES(total), time=VALUES(time)"
        self.cursor.executemany(query, df)
        self.cnx.commit()

    def update_average_over_time(self, df: List[str]) -> None:
        pass

class HourlyOverviewEventAmbientConditions(HourlyOverviewEvent):

    def get_aggregations(self, df: pd.DataFrame) -> Tuple[Union[float, None]]:
        pass

class HourlyOverviewEventAnomaly(HourlyOverviewEvent):

    def get_aggregations(self, df: pd.DataFrame) -> None:
        pass
        