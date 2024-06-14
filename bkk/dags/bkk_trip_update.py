import pendulum
from datetime import timedelta, date

import requests
from google.transit import gtfs_realtime_pb2
from pymongo import MongoClient

from airflow.decorators import dag, task
from airflow import AirflowException

key = "35e94de6-6448-4192-897c-149ed04f4f9a"
base_link = "https://go.bkk.hu/api/query/v1/ws/gtfs-rt/full"

@dag(
    "bkk_trip_updates",
    description="Budapests public transport trip data",
    catchup=False,
    start_date=pendulum.datetime(2024, 1, 1),
    tags=['bkk', 'trip updates'],
    schedule_interval = timedelta(seconds=10),
    default_args={'retries':0}
)
def bkk_trip():
    """
    ### Pipeline for BKK trip update
    This loads the API of https://go.bkk.hu/api/query/v1/ws/gtfs-rt/full/TripUpdates.pb
    and loads it in a MongoDB database
    For more documentation on the GTFS see: https://github.com/google/transit/blob/master/gtfs-realtime/proto/gtfs-realtime.proto
    """
    @task()
    def get_current_alerts():
        # TODO fetch from db and ensure it gets pulled first
        server = "mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000"
        db_name = 'bkk_data'
        col_alerts_cur = 'alert_active'

        try:
            client = MongoClient(server)
            # force connection to see if the server is running
            client.server_info()
        except:
            raise AirflowException('Couldn\'t estabilish connection with MongoDB server.')

        db = client[db_name]
        # get the still active alerts from the db
        entities = db[col_alerts_cur].find({}, {'informed_entities':1})
        #out = tuple([x.route_id for x in inf_ent])
        out = tuple([i['route_id'] for x in entities for i in x])
        client.close()
        return out

    @task()
    def transform(input, alerts):
        # there are routes which are NO_SERVICE; they come in the alert feed
        # they need to be filtered out, as they are ghosts in feed_tru
        #filter out trip_update feed from cancelled and not yet departed routes
        #no_service_routes.keys() and
        url_trip_upd = "{}/TripUpdates.pb?key={}".format(base_link, key)

        feed_tru = gtfs_realtime_pb2.FeedMessage()

        r_tru = requests.get(url_trip_upd)
        if r_tru.status_code != 200:
            raise AirflowException('could not get alerts data')

        feed_tru.ParseFromString(r_tru.content)

        tru_filtered = [x for x in feed_tru.entity \
            if not x.trip_update.trip.route_id in alerts and \
               x.trip_update.trip.schedule_relationship != 3 and \
               x.trip_update.stop_time_update[0].arrival.time <= feed_tru.header.timestamp]

        #contains all the data which is going to the db
        vhc_array = []
        for x in tru_filtered:
            if x.trip_update.vehicle.id == '':
                continue
            tu = x.trip_update
            tut = tu.trip
            data = {'meta':{'date':tut.start_date, 'trip_id':tut.trip_id, 'route_id':tut.route_id, \
                            'vehicle_id':tu.vehicle.id, 'id': x.id},
                    'next_stop_index':0, 'stop_num':len(tu.stop_time_update), 'alive':True,
                    'stops':[]}
            timestamp = tu.timestamp
            for si, s in enumerate(tu.stop_time_update):
                stop_dic = {'stop_id':s.stop_id, 'alert_id':None,
                            'arrival_timestamp':0, 'departure_timestamp':0,
                            'congestion_level':0, 'current_status':0, 'top_speed':0}
                if s.arrival.time <= timestamp:
                    stop_dic['arrival_timestamp'] = s.arrival.time
                    stop_dic['delay'] = s.arrival.delay
                    data['next_stop_index'] = si + 1
                if s.departure.time <= timestamp:
                    stop_dic['departure_timestamp'] = s.departure.time
                data['stops'].append(stop_dic)
            if data['next_stop_index'] == data['stop_num']:
                data['alive'] = False
            vhc_array.append(data)

        return vhc_array

    @task()
    def load(processed_input):
        server = "mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000"
        db_name = 'bkk_data'
        col_vehicle = 'vehicle_trip'

        try:
            client = MongoClient(server)
            client.server_info()
        except:
            raise AirflowException('Couldn\'t estabilish connection with MongoDB server.')

        db = client[db_name]
        # get the trip_id-date pairs from the db and filter what's only needed to be updated
        # FIXME are trip_ids unique to a day?
        trip_id_curs = db[col_vehicle].find({'meta.date':date.today().strftime('%Y%m%d')})
        todays_trips = [x['meta']['trip_id'] for x in trip_id_curs]

        vhc_new = []

        if len(todays_trips) == 0:
            vhc_new = processed_input
        else:
            for x in processed_input:
                if not x['meta']['trip_id'] in todays_trips:
                    vhc_new.append(x)
                    continue
                db_id_filter = {'meta.id':x['meta']['id']}
                db_alive = db[col_vehicle].find_one(db_id_filter, {'alive':1, '_id':0})
                if db_alive:
                    db[col_vehicle].update_one(db_id_filter, \
                                               {'$set':{'next_stop_index':x['next_stop_index']}})
                    db_next_stop = db[col_vehicle].find_one(db_id_filter,
                                                        {'next_stop_index':1, '_id':0})['next_stop_index']
                    for i in range(db_next_stop, x['next_stop_index']):
                        stop = x['stops'][i]
                        if stop['arrival_timestamp'] and stop['departure_timestamp'] == 0:
                            break
                        if stop['arrival_timestamp'] != 0:
                            db[col_vehicle].update_one(db_id_filter, \
                                {'$set':{'stops.'+str(i)+'.arrival_timestamp':stop['arrival_timestamp']}})
                        if stop['departure_timestamp'] != 0:
                            db[col_vehicle].update_one(db_id_filter, \
                                {'$set':{'stops.'+str(i)+'.departure_timestamp':stop['departure_timestamp']}})
                    if x['alive'] == False:
                        db[col_vehicle].update_one(db_id_filter, {'$set':{'alive':False}})

        db[col_vehicle].insert_many(vhc_new)
        client.close()

    alerts = get_current_alerts()
    processed_data = transform(0, alerts)
    load(processed_data)

bkk_trip()
