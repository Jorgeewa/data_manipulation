from data_processing.db_connection.connection import DbConnection
from data_processing.controller.factories.events import read_events_exporter
import logging
from data_processing.event_handlers.event_callables import db_events, email_events, rabbitmq_events, s3_events
from data_processing.event_handlers.event_handler import register_event, trigger_event
from data_processing.event_handlers.events import Events
from data_processing.event_handlers.rabbitmq.rabbitmq import RabbitMq
from typing import TextIO, Union
import sys
import os
from datetime import datetime
import json
import tempfile
import pika

logging.basicConfig(filename='/var/log/py/log',
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.DEBUG)


'''
    This class instantiates the data processing class and injects the dependencies into it
'''


def main() -> None:
    register_events()

    rmq = RabbitMq()
    cnx = rmq.connect("event_manager")
    channel = rmq.get_channel()

    channel.basic_consume(queue='event_manager', on_message_callback=callback)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

def callback(ch, method, properties, body) -> None:
    message = json.loads(body)
    event_processing(ch, method, message['event_type'], message['roundId'], message['robotId'], message['observableName'], message['x_dim'], message['y_dim'], message['type'])
    
    print(f" [x] Received {message['observableName']} for robot: {message['robotId']} and round: {message['roundId']}")
def event_processing(ch: pika.BlockingConnection.channel, method, event_type: str, round_id: str, robot_id: str, observable_name: str, x_dim: str, y_dim: str, type_: str) -> None:
    cnx = DbConnection().connect_to_db()
    log = logging.getLogger('events')

    # Trick to get the factory to configure derived observables well
    type__ = type_
    if event_type == Events.DERIVED_OBSERVABLE.name:
        type__ = observable_name
    p = read_events_exporter(event_type, type__)
    processor = p.get_exporter(
                    cnx=cnx, 
                    round_id=round_id,
                    robot_id=robot_id,
                    type_=type_,
                    x_dim=x_dim,
                    y_dim=y_dim, 
                    observable_name=observable_name, 
                    logging=log
                )
    msg = f"Events processing for event type: {event_type} {observable_name} for {round_id}  initiated."
    log.info(msg)
    ''' 
        I have changed my rabbitmq strategy. I always acknowledge events and frontend events because they can always be rebuilt. I only knack data processing cause when lost it can't be 
        retrieved. This will help prevent accumulation of messages but post logs when things go pear shaped.
    '''
    try:
        processor.process()
        print("worked should acknowledge", method.delivery_tag)
        
    except Exception as e:
        msg = f"Event processing of {observable_name} with round_id: {round_id}, robot_id: {robot_id} and of type: {event_type} has failed with error msg {str(e)}"
        log.exception(e)
        print(e, "didn't work should nack", method.delivery_tag)
    finally:
        ch.basic_ack(delivery_tag=method.delivery_tag)
        cnx.close()


def register_events() -> None:
    register_event(Events.FRONTEND, rabbitmq_events.trigger_frontend_events)
    register_event(Events.LATEST, rabbitmq_events.trigger_time_events)
    register_event(Events.NEW_ROUND, rabbitmq_events.trigger_time_events)
    register_event(Events.NEW_NEW_DAY, rabbitmq_events.trigger_time_events)
    register_event(Events.NEW_TIME_OF_DAY, rabbitmq_events.trigger_time_events)
    register_event(Events.NEW_HOURLY_OVERVIEW, rabbitmq_events.trigger_time_events)
    register_event(Events.ALERT, rabbitmq_events.trigger_alert_events)


def main_testing(event, type_, round_id, observable_name) -> None:
    register_events()
    cnx = DbConnection().connect_to_db()
    log = logging.getLogger('events')
    p = read_events_exporter(event, observable_name)
    processor = p.get_exporter(
                    cnx=cnx, 
                    round_id=round_id,
                    robot_id=None,
                    type_=type_,
                    xDim=0,
                    yDim=0, 
                    observable_name=observable_name, 
                    logging=log
                )
    processor.process()

if __name__ == "__main__":
    '''
        Should recieve:
        round_id: str
        robot_id: str,
        observable_name: str
        file: TextIO,
        observable_id: str,
        type: str
    # run main function
    #file = sys.argv[1]
    round_id = sys.argv[1]
    robot_id = sys.argv[2]
    observable_name = sys.argv[3]
    observable_id = sys.argv[4]
    type_ = sys.argv[5]
    register_events()
    main("temperature.csv", round_id, robot_id, observable_name, observable_id, type_)
    main_testing("DERIVED_OBSERVABLE", "ambient_condition", 'cwnjsbk', 'digestion_index')'''

    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
