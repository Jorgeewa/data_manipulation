from abc import ABC, abstractmethod
from typing import Dict
from data_processing.frontend.frontend_processing import FrontendProcessing
from data_processing.frontend.graph import GraphLatestAmbientCondition, GraphLatestAnomaly, GraphRoundAmbient, GraphRoundAnomaly, GraphNewDayAmbient, GraphNewDayAnomaly, GraphTimeofDayAmbient, GraphTimeofDayAnomaly
from data_processing.frontend.heatmap import HeatmapLatest, HeatmapNewDay, HeatmapRound, HeatmapTimeofDay
from data_processing.frontend.hourly_overview import HourlyOverviewAmbient, HourlyOverviewAnomaly
from data_processing.frontend.hourly import HourAmbient, HourAnomaly, DayAmbient, DayAnomaly, WeekAmbient, WeekAnomaly, MonthAmbient, MonthAnomaly
from data_processing.production_cycle_details.production_cycle_details import PCDetails
from data_processing.data_processing_events.data_processing_events import DPEvents
from data_processing.event_handlers.events import Events


class FrontendProcessingFactories(ABC):

    @abstractmethod
    def get_exporter(self) -> FrontendProcessing:
        pass


class ExportLatestHeatmap(FrontendProcessingFactories):

    def get_exporter(self, **kwargs: Dict[str, str]) -> FrontendProcessing:
        pc_details = PCDetails(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        data_events = DPEvents(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        return HeatmapLatest(pc_details=pc_details, data_events=data_events, **kwargs)
    
class ExportNewDayHeatmap(FrontendProcessingFactories):

    def get_exporter(self, **kwargs: Dict[str, str]) -> FrontendProcessing:
        pc_details = PCDetails(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        data_events = DPEvents(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        return HeatmapNewDay(pc_details=pc_details, data_events=data_events, **kwargs)
    
class ExportRoundHeatmap(FrontendProcessingFactories):

    def get_exporter(self, **kwargs: Dict[str, str]) -> FrontendProcessing:
        pc_details = PCDetails(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        data_events = DPEvents(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        return HeatmapRound(pc_details=pc_details, data_events=data_events, **kwargs)
    
class ExportTimeofDayHeatmap(FrontendProcessingFactories):

    def get_exporter(self, **kwargs: Dict[str, str]) -> FrontendProcessing:
        pc_details = PCDetails(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        data_events = DPEvents(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        return HeatmapTimeofDay(pc_details=pc_details, data_events=data_events, **kwargs)
    
    
class ExportGraphLatestAmbientCondition(FrontendProcessingFactories):

    def get_exporter(self, **kwargs: Dict[str, str]) -> FrontendProcessing:
        pc_details = PCDetails(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        data_events = DPEvents(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        return GraphLatestAmbientCondition(pc_details=pc_details, data_events=data_events, **kwargs)
    
class ExportGraphLatestAnomaly(FrontendProcessingFactories):

    def get_exporter(self, **kwargs: Dict[str, str]) -> FrontendProcessing:
        pc_details = PCDetails(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        data_events = DPEvents(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        return GraphLatestAnomaly(pc_details=pc_details, data_events=data_events, **kwargs)
    
    
class ExportGraphRoundAmbientCondition(FrontendProcessingFactories):

    def get_exporter(self, **kwargs: Dict[str, str]) -> FrontendProcessing:
        pc_details = PCDetails(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        data_events = DPEvents(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        return GraphRoundAmbient(pc_details=pc_details, data_events=data_events, **kwargs)
    
class ExportGraphRoundAnomaly(FrontendProcessingFactories):

    def get_exporter(self, **kwargs: Dict[str, str]) -> FrontendProcessing:
        pc_details = PCDetails(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        data_events = DPEvents(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        return GraphRoundAnomaly(pc_details=pc_details, data_events=data_events, **kwargs)
  
  
class ExportGraphNewDayAmbientCondition(FrontendProcessingFactories):

    def get_exporter(self, **kwargs: Dict[str, str]) -> FrontendProcessing:
        pc_details = PCDetails(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        data_events = DPEvents(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        return GraphNewDayAmbient(pc_details=pc_details, data_events=data_events, **kwargs)
    
class ExportGraphNewDayAnomaly(FrontendProcessingFactories):

    def get_exporter(self, **kwargs: Dict[str, str]) -> FrontendProcessing:
        pc_details = PCDetails(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        data_events = DPEvents(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        return GraphNewDayAnomaly(pc_details=pc_details, data_events=data_events, **kwargs)  
  
class ExportGraphTimeofDayAmbientCondition(FrontendProcessingFactories):

    def get_exporter(self, **kwargs: Dict[str, str]) -> FrontendProcessing:
        pc_details = PCDetails(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        data_events = DPEvents(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        return GraphTimeofDayAmbient(pc_details=pc_details, data_events=data_events, **kwargs)
    
class ExportGraphTimeofDayAnomaly(FrontendProcessingFactories):

    def get_exporter(self, **kwargs: Dict[str, str]) -> FrontendProcessing:
        pc_details = PCDetails(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        data_events = DPEvents(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        return GraphTimeofDayAnomaly(pc_details=pc_details, data_events=data_events, **kwargs)  
  
class ExportHourlyOverviewAmbientCondition(FrontendProcessingFactories):

    def get_exporter(self, **kwargs: Dict[str, str]) -> FrontendProcessing:
        pc_details = PCDetails(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        data_events = DPEvents(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        return HourlyOverviewAmbient(pc_details=pc_details, data_events=data_events, **kwargs)
    
    
class ExportHourlyOverviewAnomaly(FrontendProcessingFactories):

    def get_exporter(self, **kwargs: Dict[str, str]) -> FrontendProcessing:
        pc_details = PCDetails(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        data_events = DPEvents(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        return HourlyOverviewAnomaly(pc_details=pc_details, data_events=data_events, **kwargs)
    
    
class ExportHourAmbientCondition(FrontendProcessingFactories):

    def get_exporter(self, **kwargs: Dict[str, str]) -> FrontendProcessing:
        pc_details = PCDetails(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        data_events = DPEvents(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        return HourAmbient(pc_details=pc_details, data_events=data_events, **kwargs)
    
    
class ExportHourAnomaly(FrontendProcessingFactories):

    def get_exporter(self, **kwargs: Dict[str, str]) -> FrontendProcessing:
        pc_details = PCDetails(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        data_events = DPEvents(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        return HourAnomaly(pc_details=pc_details, data_events=data_events, **kwargs)
    
    
class ExportDayAmbientCondition(FrontendProcessingFactories):

    def get_exporter(self, **kwargs: Dict[str, str]) -> FrontendProcessing:
        pc_details = PCDetails(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        data_events = DPEvents(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        return DayAmbient(pc_details=pc_details, data_events=data_events, **kwargs)
    
    
class ExportDayAnomaly(FrontendProcessingFactories):

    def get_exporter(self, **kwargs: Dict[str, str]) -> FrontendProcessing:
        pc_details = PCDetails(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        data_events = DPEvents(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        return DayAnomaly(pc_details=pc_details, data_events=data_events, **kwargs)
    
    
    
class ExportWeekAmbientCondition(FrontendProcessingFactories):

    def get_exporter(self, **kwargs: Dict[str, str]) -> FrontendProcessing:
        pc_details = PCDetails(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        data_events = DPEvents(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        return WeekAmbient(pc_details=pc_details, data_events=data_events, **kwargs)
    
    
class ExportWeekAnomaly(FrontendProcessingFactories):

    def get_exporter(self, **kwargs: Dict[str, str]) -> FrontendProcessing:
        pc_details = PCDetails(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        data_events = DPEvents(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        return WeekAnomaly(pc_details=pc_details, data_events=data_events, **kwargs)
    
    
    
class ExportMonthAmbientCondition(FrontendProcessingFactories):

    def get_exporter(self, **kwargs: Dict[str, str]) -> FrontendProcessing:
        pc_details = PCDetails(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        data_events = DPEvents(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        return MonthAmbient(pc_details=pc_details, data_events=data_events, **kwargs)
    
    
class ExportMonthAnomaly(FrontendProcessingFactories):

    def get_exporter(self, **kwargs: Dict[str, str]) -> FrontendProcessing:
        pc_details = PCDetails(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        data_events = DPEvents(kwargs['cnx'], kwargs['round_id'], kwargs['observable_name'])
        return MonthAnomaly(pc_details=pc_details, data_events=data_events, **kwargs)
    
    

    
    
def read_frontend_exporter(event_type, type_of_plot, type_) -> FrontendProcessingFactories:

    factories = {
        Events.LATEST.name: {
            "graph": {
                "ambient_condition" : ExportGraphLatestAmbientCondition(),
                "anomaly" : ExportGraphLatestAnomaly(),
            },
            "heatmap": {
                "ambient_condition" : ExportLatestHeatmap(),
                "anomaly" : ExportLatestHeatmap(),
            },

        },
        
        Events.NEW_ROUND.name: {
            "graph": {
                "ambient_condition" : ExportGraphRoundAmbientCondition(),
                "anomaly" : ExportGraphRoundAnomaly(),
            },
            "heatmap": {
                "ambient_condition" : ExportRoundHeatmap(),
                "anomaly" : ExportRoundHeatmap(),
            },

        },
        
        Events.NEW_TIME_OF_DAY.name: {
            "graph": {
                "ambient_condition" : ExportGraphTimeofDayAmbientCondition(),
                "anomaly" : ExportGraphTimeofDayAnomaly(),
            },
            "heatmap": {
                "ambient_condition" : ExportTimeofDayHeatmap(),
                "anomaly" : ExportTimeofDayHeatmap(),
            },

        },
        
        Events.NEW_NEW_DAY.name: {
            "graph": {
                "ambient_condition" : ExportGraphNewDayAmbientCondition(),
                "anomaly" : ExportGraphNewDayAnomaly(),
            },
            "heatmap": {
                "ambient_condition" : ExportNewDayHeatmap(),
                "anomaly" : ExportNewDayHeatmap(),
            },

        },
        
        Events.NEW_HOURLY_OVERVIEW.name: {
            "hourly_overview": {
                "ambient_condition" : ExportHourlyOverviewAmbientCondition(),
                "anomaly" : ExportHourlyOverviewAnomaly(),
            },
            "hour": {
                "ambient_condition" : ExportHourAmbientCondition(),
                "anomaly" : ExportHourAnomaly(),
            },
            "week": {
                "ambient_condition" : ExportWeekAmbientCondition(),
                "anomaly" : ExportWeekAnomaly(),
            },
            "month": {
                "ambient_condition" : ExportMonthAmbientCondition(),
                "anomaly" : ExportMonthAnomaly(),
            },
            "day": {
                "ambient_condition" : ExportDayAmbientCondition(),
                "anomaly" : ExportDayAnomaly(),
            },

        },
        
    }

    return factories[event_type][type_of_plot][type_]