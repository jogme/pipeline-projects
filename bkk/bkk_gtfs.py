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
#col_routes = 'routes'

try:
    pass #client = MongoClient(server)
except:
    print('Couldn\'t estabilish connection with MongoDB server.')

#db = client[db_name]

# %%
# create docs to include to the db
#vhc_data = {'meta':{'date':0, 'timestamp':0, 'trip_id':0, 'route_id':0}, 'next_stop_index':0, \
#            'stops':[{'stop_id':0, 'delay':0, 'alert_id':None, 'arrival_timestamp':0, 'congestion_level':0,
#                     'current_status':0, 'top_speed':0}],
#            'vehicle_label':0}

#contains all the data which is going to the db
# TODO
# - filter what is already in the db and needs only an update
# - what are the things that may change and need an update?
vhc_array = []
for x in feed_tru.entity:
    if x.trip_update.vehicle.id == '':
        continue
    tu = x.trip_update
    tut = tu.trip
    data = {'meta':{'date':tut.start_date, 'trip_id':tut.trip_id, 'route_id':tut.route_id, \
                    'vehicle_id':tu.vehicle.id},
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
    vhc_array.append(data)

#print(vhc_array)

# get the trip_id-date pairs from the db and filter what's only needed to be updated
# FIXME are trip_ids unique to a day?
#trip_id_curs = db[col_vehicle].find({'meta.date':date.today().strftime('%Y%m%d')},
#                                    {'meta.trip_id':1, '_id':0})
#todays_trips = [x['meta']['trip_id'] for x in trip_id_curs]
todays_trips = []

vhc_new = []

if len(todays_trips) == 0:
    vhc_new = vhc_array
else:
    for x in vhc_array:
        if not x['meta']['trip_id'] in todays_trips:
            vhc_new.append(x)
            continue
        # TODO just update
        if x['alive']:
            if x['next_stop_index'] == x['stop_num']:
                x['alive'] = False
                continue
            for i in range(x['next_stop_index'], x['stop_num']):
                stop = x['stops'][i]
                #if stop['arrival_timestamp'] <=

#db[col_vehicle].insertMany(vhc_new)
#client.close()
