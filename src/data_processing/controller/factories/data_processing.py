from abc import ABC, abstractmethod
from typing import List, Tuple, Union, Callable, Any, Dict, TextIO
from data_processing.data_cleaning.data_processing import DataProcessing, DataProcessingAmbientCondition, DataProcessingAnomaly
from data_processing.production_cycle_details.production_cycle_details import PCDetails
from data_processing.data_processing_events.data_processing_events import DPEvents
from data_processing.download.download import DownloadMainTablePandas

class DataProcessingFactories(ABC):

    @abstractmethod
    def get_exporter(self) -> DataProcessing:
        pass
    
    
class ExportAmbientConditions(DataProcessingFactories):

    def get_exporter(self, **kwargs: Dict[str, str]) -> DataProcessing:
        download = DownloadMainTablePandas(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        pc_details = PCDetails(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        data_events = DPEvents(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        return DataProcessingAmbientCondition(
                                    download=download, 
                                    pc_details=pc_details, 
                                    data_events=data_events,
                                    **kwargs
                                    )


class ExportAnomaly(DataProcessingFactories):

    def get_exporter(self, **kwargs: Dict[str, str]) -> DataProcessing:
        download = DownloadMainTablePandas(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        pc_details = PCDetails(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        data_events = DPEvents(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        return DataProcessingAnomaly(
                                    download=download, 
                                    pc_details=pc_details, 
                                    data_events=data_events,
                                    **kwargs
                                    )


def read_data_processing_exporter(type_) -> DataProcessingFactories:

    factories = {
        "ambient_condition" : ExportAmbientConditions(),
        "anomaly" : ExportAnomaly(),
    }

    return factories[type_]