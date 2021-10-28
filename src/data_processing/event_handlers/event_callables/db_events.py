from random import random, choice
import string
from typing import Dict
from datetime import datetime


def invalid_alert(self, params: Dict[str, str]):
    '''
        This function updates the alert notification table when data has been deleted

        Returns None

        Parameters
        ----------
        time: start time of deleted round.
        round_number: round number of deleted round

        Returns
        --------
        None
    '''
    # update alert notification.
    message = f"Round with id {params['round_id']}, at time - {params['time']} and number {params['round_number']} has been discarded."
    subject = "Discarded robot round"
    notification_method_id = "none"
    alert_id = "0000000"
    id_ = generate_random_id(params, 'round_invalidation')
    level = "high"
    user_id = "0000000"
    event_type = "robot round discarded"
    created_by = "0000000"
    user_id = "0000000"
    params = (id_, params['round_id'], alert_id, notification_method_id, user_id, params['time'], subject, message, level, created_by, event_type)
    query = "insert into alert_notification (id, robot_id, alert_id, notification_method_id, user_id, time, subject, message, level, created_by, event_type) values \
        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    params['cnx'].cursor.execute(query, params)
    params['cnx'].commit()


def generate_random_id(self, params: Dict[str, str], col: str):
    '''
        This function creates a 7 digit random id for the global_id table

        Returns id

        Parameters
        ----------
        None

        Returns
        --------
        id
    '''
    id_ = ''.join(choice(string.ascii_lowercase + string.digits) for _ in range(7))
    query = f"Select id from global_id where id = '{id_}'"
    params['cnx'].cursor.execute(query)
    res = params['cnx'].cursor.fetchone()
    while res is not None:
        id_ = ''.join(choice(string.ascii_uppercase + string.ascii_lowercase + string.digits) for _ in range(7))
        query = f"Select id from global_id where id = '{id_}'"
        params['cnx'].cursor.execute(query)
        res = params['cnx'].cursor.fetchone()

    params_ = (id_,  'round_invalidation', '0000000')
    query = "insert into global_id (id, type, created_by) values \
        (%s, %s, %s)"
    params['cnx'].cursor.execute(query, params_)
    params['cnx'].commit()
    return id_



def update_measurement(self, params: Dict[str, str]):
    '''
        This function updates the measurement table

        Returns None

        Parameters
        ----------
        time: start time of deleted round.
        round_number: round number of deleted round

        Returns
        --------
        None
    '''
    # update alert notification.
    id_ = self.generate_random_id(params, 'measurement')
    now = datetime.now().strftime("%y-%m-%d %H:%M:%S")
    created_by = "0000000"
    measurement = (id_, params['round_id'], params['url'], now, now, params['observable_id'], created_by)
    query = "insert into alert_notification (id, robot_id, url, from, to, observable_id, created_by) values \
        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    params['cnx'].cursor.execute(query, params)
    params['cnx'].commit()
