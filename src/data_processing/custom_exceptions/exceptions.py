
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
        body = body_text = f"Robot with id {self.robot_id} is sending suspicious data for round id {self.round_id}"
        recipients = ["xxx@xxx.com", "xxx@xxx.com"]
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
        body = body_text = f"Robot with id {self.robot_id} has sent a bad file for round id {self.round_id}"
        recipients = ["xxx@xxx.com", "xxx@xxx.com", "xxx@xxx.com", "xxx@xxx.com"]
        trigger_event(Events.SUSPICIOUS_DATA, {"round_id": self.round_id, "subject": self.message, "file": self.file, "body": body, "recipients": recipients})

