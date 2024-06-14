import pendulum
from datetime import timedelta, date, datetime
import time

import requests
from google.transit import gtfs_realtime_pb2
from pymongo import MongoClient
from pymongo.errors import BulkWriteError

from airflow.decorators import dag, task
from airflow import AirflowException

key = "35e94de6-6448-4192-897c-149ed04f4f9a"
base_link = "https://go.bkk.hu/api/query/v1/ws/gtfs-rt/full"
server = "mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000"
db_name = 'bkk_data'
col_alerts_cur = 'alert_active'
col_alerts_arch = 'alert_archive'

@dag(
    "bkk_alerts",
    description="Budapests public transport alerts data",
    catchup=False,
    start_date=pendulum.datetime(2024, 1, 1),
    tags=['bkk', 'alerts'],
    schedule_interval = timedelta(minutes=30),
    default_args={'retries':0}
)
def bkk_alerts():
    """
    ### Pipeline for BKK alerts
    This loads the API of https://go.bkk.hu/api/query/v1/ws/gtfs-rt/full/Alerts.pb
    and loads it in a MongoDB database
    For more documentation on the GTFS see: https://github.com/google/transit/blob/master/gtfs-realtime/proto/gtfs-realtime.proto
    """
    @task()
    def transform():
        """
        save as:
        d = {'bkk_id':0, 'time_start':0, 'time_end':0, 'cause':0, 'effect':0, 'header_text':0,
             'descriptive_text:0, 'informed_entities':[{'route_id':0, 'stop_id':)}]}
        """
        url_alerts = "{}/Alerts.pb?key={}".format(base_link, key)

        feed_ale = gtfs_realtime_pb2.FeedMessage()

        r_ale = requests.get(url_alerts)
        if r_ale.status_code != 200:
            raise AirflowException('could not get alerts data')

        feed_ale.ParseFromString(r_ale.content)

        output = []
        for x in feed_ale.entity:
            a = x.alert
            output.append({'bkk_id':x.id, 'time_start':a.active_period[0].start,
                           'time_end':a.active_period[0].end, 'cause':a.cause, 'effect':a.effect,
                           'header_text':a.header_text.translation[0].text,
                           'descriptive_text':a.description_text.translation[0].text,
                           'ttl_date': datetime.fromtimestamp(a.active_period[0].end),
                           'informed_entities':
                           [{'route_id':i.route_id, 'stop_id':i.stop_id} for i in a.informed_entity]})
        return output

    @task()
    def remove_old(processed_input):
        try:
            client = MongoClient(server)
            client.server_info()
        except:
            raise AirflowException('Couldn\'t estabilish connection with MongoDB server.')

        db = client[db_name]
        cur_bkk_ids = tuple([x['bkk_id'] for x in processed_input])
        alert_to_del = [x['bkk_id'] for x in db[col_alerts_cur].find({'time_end':{'$eq':0}}, {'bkk_id':1}) if not x['bkk_id'] in cur_bkk_ids]
        if len(alert_to_del) != 0:
            db[col_alerts_cur].delete_many({'bkk_id':{'$in':alert_to_del}})

        client.close()

    @task()
    def load(processed_input):
        try:
            client = MongoClient(server)
            client.server_info()
        except:
            raise AirflowException('Couldn\'t estabilish connection with MongoDB server.')

        db = client[db_name]
        # get the still active alerts from the db
        cur_alerts = tuple([x['bkk_id'] for x in db[col_alerts_cur].find({}, {'bkk_id':1})])
        if len(cur_alerts) == 0:
            data_to_insert = processed_input
        else:
            data_to_insert = [x for x in processed_input if not x['bkk_id'] in cur_alerts]
        ttl_index = {x['bkk_id']:{'expireAfterSeconds'
                     :x['time_end'] - int(time.mktime(datetime.now().timetuple()))}
                    for x in data_to_insert}

        if len(data_to_insert) > 0:
            """
            # These indexes are needed to be specified on the "alert_active" and "alert_archive"
            # collections to work properly. The indexes are needed to be defined only once, therefore
            # excluding them from the script.
            # to kill the old alerts
            db[col_alerts_cur].create_index(['ttl_date'], name='timout death',
                                            expireAfterSeconds=20, background=True,
                                            partialFilterExpression={'time_end':{'$gt':0}})
            # this index prevents duplicate items in the archive
            db[col_alerts_arch].create_index(['bkk_id'], unique=True)
            """
            db[col_alerts_cur].insert_many(data_to_insert)
            try:
                db[col_alerts_arch].insert_many(data_to_insert, ordered=False)
            except BulkWriteError:
                # this error is raised when non-unique value is tried to be inserted too
                # let's just ignore it
                pass
        client.close()
    
    processed_data = transform()
    remove_old(processed_data)
    load(processed_data)

bkk_alerts()
