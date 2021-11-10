
from data_processing.event_handlers.events import Events
from data_processing.event_handlers.event_handler import trigger_event
from typing import TextIO


class CsvException(Exception):
    pass


class SuspiciousDataException(Exception):

    def __init__(self, round_id: str, observable_name: str, message: str, file: TextIO):
        self.observable_name = observable_name
        self.round_id = round_id
        self.message = message
        self.file = file
        self.post()
        super().__init__(message)

    def post(self):
        """ send email about suspicious data being sent"""
        if self.observable_name != "temperature":
            return
        body = f"Robot with id {self.robot_id} is sending suspicious data for round id {self.round_id}"
        recipients = ["xxx", "xxx"]
        trigger_event(Events.SUSPICIOUS_DATA, {"round_id": self.round_id, "subject": self.message, "file": self.file, "body": body, "recipients": recipients})

class InvalidRound(Exception):

    def __init__(self, round_id: str, observable_name: str, message: str):
        self.observable = observable_name
        self.round_id = round_id
        self.message = message
        super().__init__(self.message)


class ObservableNotAvailable(Exception):

    def __init__(self, round_id: str, observable_name: str, message: str):
        self.observable = observable_name
        self.round_id = round_id
        self.message = message
        super().__init__(self.message)


class BadFile(Exception):

    def __init__(self, round_id: str, observable_name: str, message: str, file: TextIO):
        self.observable_name = observable_name
        self.round_id = round_id
        self.message = message
        self.file = file
        self.post()
        super().__init__(self.message)


    def post(self):
        """ send email about bad file being sent"""
        body = f"Robot with id {self.robot_id} has sent a bad file for round id {self.round_id}"
        recipients = ["xxx", "xxx", "xxx", "xxx"]
        trigger_event(Events.SUSPICIOUS_DATA, {"round_id": self.round_id, "subject": self.message, "file": self.file, "body": body, "recipients": recipients})
        
        
class EmptyFrontendException(Exception):
    
    def __init__(self, round_id: str, observable_name: str, message: str):
        self.observable = observable_name
        self.round_id = round_id
        self.message = message
        super().__init__(self.message)
        
        
class WrongCordinatesException(Exception):
    
    def __init__(self, round_id: str, x_dim: int, y_dim: int, message: str):
        self.round_id = round_id
        self.x_dim = x_dim
        self.y_dim = y_dim
        self.message = message
        self.file = None
        self.post(x_dim, y_dim, round_id)
        super().__init__(self.message)


    def post(self):
        """ send email about bad file being sent"""
        body = f"Round with id {self.round_id} has sent data with wrong cordinates and frontend data can't be plotted. X cordinates = {self.x_dim}, Y cordinates = {self.y_dim}"
        recipients = ["xxx", "xxx", "xxx", "xxx"]
        trigger_event(Events.SUSPICIOUS_DATA, {"round_id": self.round_id, "subject": self.message, "file": self.file, "body": body, "recipients": recipients})
