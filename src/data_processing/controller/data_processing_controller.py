from data_processing.db_connection.connection import DbConnection
from data_processing.download.download import DownloadMainTablePandas
from data_processing.production_cycle_details.production_cycle_details import PCDetails
from data_processing.data_processing_events.data_processing_events import DPEvents
import logging
from data_processing.data_cleaning.data_processing import DataProcessingAmbientCondition, DataProcessingAnomaly
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
    cnx = rmq.connect("data_processing")
    channel = rmq.get_channel()

    channel.basic_consume(queue='data_processing', on_message_callback=callback)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

def callback(ch, method, properties, body) -> None:
    message = json.loads(body)
    temp = tempfile.NamedTemporaryFile(mode='w+t', delete=True)
    try:
        temp.writelines(message['file_content'])
        temp.seek(0)
        data_processing(ch, method, temp.name, message['roundId'], message['robotId'], message['observableName'], message['observableId'], message['type'], message['dataFile'])
    finally:
        temp.close()
    
    print(f" [x] Received {message['observableName']} for robot: {message['robotId']} and round: {message['roundId']}")
def data_processing(ch: pika.BlockingConnection.channel, method, file: Union[str, TextIO], round_id: str, robot_id: str, observable_name: str, observable_id: str, type_: str, s3_path: str) -> None:
    cnx = DbConnection().connect_to_db()
    log = logging.getLogger('data_processing')
    download = DownloadMainTablePandas(cnx, round_id, observable_name)
    pc_details = PCDetails(cnx, round_id, observable_name)
    data_events = DPEvents(cnx, round_id, observable_name)
    msg = f"Data processing for {observable_name} for {round_id}  initiated."
    log.info(msg)

    # test ambient conditions first. Use factory to set up ambient conditions and anomalies
    if type_ == "ambient_conditions":
        dp = DataProcessingAmbientCondition(
                                        cnx=cnx, 
                                        file=file, 
                                        round_id=round_id, 
                                        robot_id=robot_id,
                                        observable_name=observable_name, 
                                        observable_id=observable_id,
                                        type_=type_, 
                                        download=download, 
                                        pc_details=pc_details, 
                                        data_events=data_events,
                                        logging=log)
    else:
        dp = DataProcessingAnomaly(
                                        cnx=cnx, 
                                        file=file, 
                                        round_id=round_id, 
                                        robot_id=robot_id,
                                        observable_name=observable_name, 
                                        observable_id=observable_id,
                                        type_=type_, 
                                        download=download, 
                                        pc_details=pc_details, 
                                        data_events=data_events,
                                        logging=log)
    try:
        df = dp.read_file()
        df = dp.parse_data(df)
        df = df.values.tolist()
        params = {"round_number": df[-1][3], 'day': df[-1][7], 'time': datetime.strptime(df[-1][5], "%Y-%m-%d %H:%M:%S")}
        events = dp.check_events(params)
        dp.upsert(df)
        dp.post_time_events(**events)
        dp.post_derived_observables()
        cnx.close()
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        msg = f"Processing of {observable_name} has failed with error msg {str(e)}"
        log.exception(e)
        ch.basic_nack(delivery_tag=method.delivery_tag)
    finally:
        trigger_event(Events.FILE_UPLOADED, { "round_id": round_id, "object_name": s3_path, "observable_id": observable_id, "file": file, 'cnx': cnx})
        cnx.close()


def register_events() -> None:
    register_event(Events.INVALID_ROUND, db_events.invalid_alert)
    register_event(Events.SUSPICIOUS_DATA, email_events.suspicious_data)
    register_event(Events.NEW_ROUND, rabbitmq_events.trigger_time_events)
    register_event(Events.NEW_NEW_DAY, rabbitmq_events.trigger_time_events)
    register_event(Events.NEW_TIME_OF_DAY, rabbitmq_events.trigger_time_events)
    register_event(Events.NEW_HOURLY_OVERVIEW, rabbitmq_events.trigger_time_events)
    register_event(Events.DERIVED_OBSERVABLE, rabbitmq_events.trigger_time_events)
    register_event(Events.FILE_UPLOADED, s3_events.upload_file_s3)
    register_event(Events.FILE_UPLOADED, db_events.update_measurement)



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
    main("temperature.csv", round_id, robot_id, observable_name, observable_id, type_)'''
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)