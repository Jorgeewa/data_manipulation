from abc import ABC, abstractmethod
from typing import List, Tuple, Union, Callable, Any, Dict, TextIO
from data_processing.data_cleaning.data_processing import DataProcessing, DataProcessingAmbientCondition, DataProcessingAnomaly
from data_processing.production_cycle_details.production_cycle_details import PCDetails
from data_processing.data_processing_events.data_processing_events import DPEvents
from data_processing.data_processing_events.derived_observables_processing import DerivedObservablesProcessing
from data_processing.download.download import DownloadMainTablePandas, DownloadRound, DownloadNewDayAmbientCondition, DownloadNewDayAnomaly, DownloadTimeofDayAmbientCondition, DownloadTimeofDayAnomaly, DownloadHourlyOverviewAmbientCondition, DownloadHourlyOverviewAnomaly, DownloadLatest 
from data_processing.internal_events.latest import LatestEventAmbientConditions, LatestEventAnomaly
from data_processing.internal_events.new_day import NewDayEventAmbientConditions, NewDayEventAnomaly
from data_processing.internal_events.round import RoundEventAmbientConditions, RoundEventAnomaly
from data_processing.internal_events.time_of_day import TimeofDayEventAmbientConditions, TimeofDayEventAnomaly
from data_processing.internal_events.hourly_overview import HourlyOverviewEventAmbientConditions, HourlyOverviewEventAnomaly
from data_processing.derived_observables.derived_observables import DigestionIndex, EffectiveTemperature, Humidex, HeatStressIndex



class EventFactories(ABC):

    @abstractmethod
    def get_exporter(self) -> DataProcessing:
        pass


''' Events configuration here '''

class ExportLatestAmbientCondition(EventFactories):

    def get_exporter(self, **kwargs: Dict[str, str]) -> DataProcessing:
        download = DownloadLatest(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        pc_details = PCDetails(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        data_events = DPEvents(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        return LatestEventAmbientConditions(
                                    download=download, 
                                    pc_details=pc_details, 
                                    data_events=data_events,
                                    **kwargs
                                    )

class ExportLatestAnomaly(EventFactories):

    def get_exporter(self, **kwargs: Dict[str, str]) -> DataProcessing:
        download = DownloadLatest(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        pc_details = PCDetails(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        data_events = DPEvents(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        return LatestEventAnomaly(
                                    download=download, 
                                    pc_details=pc_details, 
                                    data_events=data_events,
                                    **kwargs
                                    )


class ExportRoundAmbientCondition(EventFactories):

    def get_exporter(self, **kwargs: Dict[str, str]) -> DataProcessing:
        download = DownloadRound(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        pc_details = PCDetails(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        data_events = DPEvents(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        return RoundEventAmbientConditions(
                                    download=download, 
                                    pc_details=pc_details, 
                                    data_events=data_events,
                                    **kwargs
                                    )


class ExportRoundAnomaly(EventFactories):

    def get_exporter(self, **kwargs: Dict[str, str]) -> DataProcessing:
        download = DownloadRound(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        pc_details = PCDetails(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        data_events = DPEvents(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        return RoundEventAnomaly(
                                    download=download, 
                                    pc_details=pc_details, 
                                    data_events=data_events,
                                    **kwargs
                                    )


class ExportTimeofDayAmbientCondition(EventFactories):

    def get_exporter(self, **kwargs: Dict[str, str]) -> DataProcessing:
        download = DownloadTimeofDayAmbientCondition(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        pc_details = PCDetails(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        data_events = DPEvents(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        return TimeofDayEventAmbientConditions(
                                    download=download, 
                                    pc_details=pc_details, 
                                    data_events=data_events,
                                    **kwargs
                                    )

class ExportTimeofDayAnomaly(EventFactories):

    def get_exporter(self, **kwargs: Dict[str, str]) -> DataProcessing:
        download = DownloadTimeofDayAnomaly(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        pc_details = PCDetails(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        data_events = DPEvents(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        return TimeofDayEventAnomaly(
                                    download=download, 
                                    pc_details=pc_details, 
                                    data_events=data_events,
                                    **kwargs
                                    )

class ExportNewDayAmbientCondition(EventFactories):

    def get_exporter(self, **kwargs: Dict[str, str]) -> DataProcessing:
        download = DownloadNewDayAmbientCondition(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        pc_details = PCDetails(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        data_events = DPEvents(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        return NewDayEventAmbientConditions(
                                    download=download, 
                                    pc_details=pc_details, 
                                    data_events=data_events,
                                    **kwargs
                                    )

class ExportNewDayAnomaly(EventFactories):

    def get_exporter(self, **kwargs: Dict[str, str]) -> DataProcessing:
        download = DownloadNewDayAnomaly(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        pc_details = PCDetails(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        data_events = DPEvents(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        return NewDayEventAnomaly(
                                    download=download, 
                                    pc_details=pc_details, 
                                    data_events=data_events,
                                    **kwargs
                                    )



class ExportHourlyOverviewAmbientCondition(EventFactories):

    def get_exporter(self, **kwargs: Dict[str, str]) -> DataProcessing:
        download = DownloadHourlyOverviewAmbientCondition(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        pc_details = PCDetails(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        data_events = DPEvents(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        return HourlyOverviewEventAmbientConditions(
                                    download=download, 
                                    pc_details=pc_details, 
                                    data_events=data_events,
                                    **kwargs
                                    )

class ExportHourlyOverviewAnomaly(EventFactories):

    def get_exporter(self, **kwargs: Dict[str, str]) -> DataProcessing:
        download = DownloadHourlyOverviewAnomaly(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        pc_details = PCDetails(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        data_events = DPEvents(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        return HourlyOverviewEventAnomaly(
                                    download=download, 
                                    pc_details=pc_details, 
                                    data_events=data_events,
                                    **kwargs
                                    )

class ExportHumidex(EventFactories):

    def get_exporter(self, **kwargs: Dict[str, str]) -> DataProcessing:
        data_events = DerivedObservablesProcessing(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        return Humidex(dobp_events=data_events, **kwargs)

class ExportHeatStressIndex(EventFactories):

    def get_exporter(self, **kwargs: Dict[str, str]) -> DataProcessing:
        data_events = DerivedObservablesProcessing(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        return HeatStressIndex(dobp_events=data_events, **kwargs)


class ExportEffectiveTemperature(EventFactories):

    def get_exporter(self, **kwargs: Dict[str, str]) -> DataProcessing:
        data_events = DerivedObservablesProcessing(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        return EffectiveTemperature(dobp_events=data_events, **kwargs)

class ExportDigestionIndex(EventFactories):

    def get_exporter(self, **kwargs: Dict[str, str]) -> DataProcessing:
        data_events = DerivedObservablesProcessing(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        return DigestionIndex(dobp_events=data_events, **kwargs)


def read_events_exporter(event, type_) -> EventFactories:
    factories = {
        "LATEST" : {
            "ambient_condition": ExportLatestAmbientCondition(),
            "anomaly": ExportLatestAnomaly(),
        },
        "ROUND" : {
            "ambient_condition": ExportRoundAmbientCondition(),
            "anomaly": ExportRoundAnomaly(),
        },
        "TIME_OF_DAY" : {
            "ambient_condition": ExportTimeofDayAmbientCondition(),
            "anomaly": ExportTimeofDayAnomaly(),
        },
        "NEW_DAY" : {
            "ambient_condition": ExportNewDayAmbientCondition(),
            "anomaly": ExportNewDayAnomaly(),
        },
        "HOURLY_OVERVIEW" : {
            "ambient_condition": ExportHourlyOverviewAmbientCondition(),
            "anomaly": ExportHourlyOverviewAnomaly(),
        },
        "DERIVED_OBSERVABLE" : {
            "humidex": ExportHumidex(),
            "heat_stress_index": ExportHeatStressIndex(),
            "effective_temperature": ExportEffectiveTemperature(),
            "digestion_index": ExportDigestionIndex(),
        }
    }
    return factories[event][type_]