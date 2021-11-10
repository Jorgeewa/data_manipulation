from enum import Enum, auto


class Events(Enum):
    SUSPICIOUS_DATA = auto()
    INVALID_ROUND = auto()
    LATEST = auto()
    NEW_ROUND = auto()
    NEW_NEW_DAY = auto()
    NEW_TIME_OF_DAY = auto()
    NEW_HOURLY_OVERVIEW = auto()
    NEW_HOURLY = auto()
    DERIVED_OBSERVABLE = auto()
    FILE_UPLOADED = auto()
    FRONTEND = auto()
    ALERT = auto()