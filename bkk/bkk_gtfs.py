import requests
from google.transit import gtfs_realtime_pb2
from pymongo import MongoClient
from datetime import date

key = "35e94de6-6448-4192-897c-149ed04f4f9a"
url_vehicle_pos = "https://go.bkk.hu/api/query/v1/ws/gtfs-rt/full/VehiclePositions.pb?key={}".format(key)
url_trip_upd = "https://go.bkk.hu/api/query/v1/ws/gtfs-rt/full/TripUpdates.pb?key={}".format(key)
url_alerts = "https://go.bkk.hu/api/query/v1/ws/gtfs-rt/full/Alerts.pb?key={}".format(key)

feed_vhc = gtfs_realtime_pb2.FeedMessage()
feed_tru = gtfs_realtime_pb2.FeedMessage()
feed_ale = gtfs_realtime_pb2.FeedMessage()

# get all the data
r_vhc = requests.get(url_vehicle_pos)
r_tru = requests.get(url_trip_upd)
r_ale = requests.get(url_alerts)

if r_vhc.status_code != 200:
    print('could not get vehicle position data')
if r_tru.status_code != 200:
    print('could not get trip update data')
if r_ale.status_code != 200:
    print('could not get alerts data')

feed_vhc.ParseFromString(r_vhc.content)
feed_tru.ParseFromString(r_tru.content)
feed_ale.ParseFromString(r_ale.content)

server = "mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000"
db_name = 'bkk_data'
col_detours = 'detours'
col_vehicle = 'vehicle_trip'
col_alerts_cur = 'alert_active'
col_alerts_arch = 'alert_archive'
#col_weather = 'weather_data'
# static data below
#col_stops = 'stops'

try:
    client = MongoClient(server)
except:
    print('Couldn\'t estabilish connection with MongoDB server.')
    exit(-1)

db = client[db_name]

# %%
# create docs to include to the db
#vhc_data = {'meta':{'date':0, 'timestamp':0, 'trip_id':0, 'route_id':0}, 'next_stop_index':0, \
#            'stops':[{'stop_id':0, 'delay':0, 'alert_id':None, 'arrival_timestamp':0, 'congestion_level':0,
#                     'current_status':0, 'top_speed':0}],
#            'vehicle_label':0}


# there are routes which are NO_SERVICE; they come in the alert feed
# they need to be filtered out, as they are ghosts in feed_tru
no_service_routes = {}
# effect 1 is NO_SERVICE
for x in [x for x in feed_ale.entity if x.alert.effect == 1]:
    r_id = list(set([ie.route_id for ie in x.alert.informed_entity]))
    no_service_routes.update({r:[x.alert.active_period[0].start, x.alert.active_period[0].end] for r in r_id})

#filter out trip_update feed from cancelled and not yet departed routes
tru_filtered = [x for x in feed_tru.entity \
    if not x.trip_update.trip.route_id in no_service_routes.keys() and \
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

#print(vhc_array)

# get the trip_id-date pairs from the db and filter what's only needed to be updated
# FIXME are trip_ids unique to a day?
trip_id_curs = db[col_vehicle].find({'meta.date':date.today().strftime('%Y%m%d')})
todays_trips = [x['meta']['trip_id'] for x in trip_id_curs]

vhc_new = []

if len(todays_trips) == 0:
    vhc_new = vhc_array
else:
    for x in vhc_array:
        if not x['meta']['trip_id'] in todays_trips:
            vhc_new.append(x)
            continue
        db_id_filter = {'meta.id':x['meta']['id']}
        db_alive = db[col_vehicle].find_one(db_id_filter, {'alive':1, '_id':0})
        if db_alive:
            db[col_vehicle].update_one(db_id_filter, \
                                       {'$set':{'next_stop_index':x['next_stop_index']}})
            db_next_stop = db[col_vehicle].find_one(db_id_filter, {'next_stop_index':1, '_id':0})['next_stop_index']
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

print('Successfully added ', len(vhc_new), ' and updated ', len(vhc_array)-len(vhc_new),' items')
