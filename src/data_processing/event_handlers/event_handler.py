from typing import Callable, Dict


events = {}


def register_event(event: str, callable: Callable[[Dict[str, str]], None]) -> None:
    
    if not event in events:
        events[event] = [ callable ]
    else:
        events[event].append(callable)

def trigger_event(event: str, params: Dict[str, str]) -> None:
    
    for callable in events[event]:
        return callable(event, params)