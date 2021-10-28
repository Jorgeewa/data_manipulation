from data_processing.event_handlers.rabbitmq.rabbitmq import RabbitMq
from data_processing.event_handlers.events import Events
from typing import Dict
import json

ROUTING_KEY = {
    Events.NEW_ROUND: 'round.event',
    Events.NEW_NEW_DAY: 'new_day.event',
    Events.NEW_TIME_OF_DAY: 'time_of_day.event',
    Events.NEW_HOURLY_OVERVIEW: 'hourly.event',
    Events.NEW_HOURLY: 'hour.event',
    Events.DERIVED_OBSERVABLE: 'derived_observable.event'
}

def trigger_time_events(event: str, body: Dict[str, str]) -> None:
    ''' connect to rabbitmq and create round, time of day, new day or daily event '''
    print(f"rabbitmq getting notified of {event} with params: {body}")
    rmq = RabbitMq()
    cnx = rmq.connect('event_manager')
    channel = rmq.get_channel()
    channel.basic_publish(exchange='chickenboy', routing_key=ROUTING_KEY[event], body=json.dumps(body))
    channel.close()
    cnx.close()

def connect_rabbitmq() -> None:
    pass
