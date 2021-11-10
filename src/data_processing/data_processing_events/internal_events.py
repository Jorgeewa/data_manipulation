from abc import ABC, abstractmethod
from typing import List
from abc import ABC, abstractmethod
from typing import List, Tuple, Union, Callable, Any, Dict
from data_processing.data_processing_events.data_processing_events import DPEvents
import pandas as pd




class InternalEvents(DPEvents):

    def __init__(self, cnx, round_id, observable_name):
        super().__init__(cnx, round_id, observable_name)