import pandas as pd
import numpy as np
import os, sys, shutil, math, h5py
os.chdir('H:\Tableau\GTFS')

year_dict = {2018: '/metro_2018_5/', 2017: '/metro_2017_4/',
             2016: '/metro_2016_5/', 2015: '/metro_2015_5/',
             2014: '/metro_2014_5/'}

year = 2018

# get GTFS data
route = pd.read_csv('H:/Tableau/GTFS/Metro_GTFS_data' + year_dict[year] +'routes.txt')
trip = pd.read_csv('H:/Tableau/GTFS/Metro_GTFS_data' + year_dict[year] + 'trips.txt')
stop = pd.read_csv('H:/Tableau/GTFS/Metro_GTFS_data' + year_dict[year] + 'stops.txt')
stop_time = pd.read_csv('H:/Tableau/GTFS/Metro_GTFS_data' + year_dict[year] + 'stop_times.txt')
# shape = pd.read_csv(r'H:\Tableau\GTFS\Metro_GTFS_data\shapes.txt')

# get the trip with maximum stops for every route, as the representative trip for that route
trip_groupby= stop_time.groupby(['trip_id']).count()
trip_groupby['trip_id'] = trip_groupby.index
trip_stop_seq_dict = dict(zip(trip_groupby['trip_id'],trip_groupby['stop_sequence']))

route_trip_dict = trip.groupby('route_id')['trip_id'].apply(list).to_dict()

route_trip_max_seq_dict = {}
for r in route_trip_dict: 
     trip_list = route_trip_dict[r]
     j = 0
     for t in trip_list:
         if trip_stop_seq_dict[t]> j:
             j = trip_stop_seq_dict[t]
             route_trip_max_seq_dict[r] = t
         else:
             continue

# frame route id and its representative trip id, so it would be the core dataframe to all other information to join 
seed_df = pd.DataFrame.from_dict(route_trip_max_seq_dict.items())
seed_df.columns = ['route_id', 'trip_id']

# get stop id and stop sequency numbers 0-N for every representative trip
route_to_stopid= pd.merge(seed_df, stop_time, how='left', on=['trip_id'])
route_to_stopid.sort_values(by=['trip_id', 'stop_sequence'])
# find stop frequency numbers don't line up perfectly,there are some gaps within it. so have to sign a list of new numbers as the new stop frequency. 
ordered_route_to_stopid = route_to_stopid[:0]
trip_list = np.unique(route_to_stopid['trip_id'])
for t in trip_list: 
    my_trip = route_to_stopid[route_to_stopid['trip_id'] == t]
    my_trip = my_trip.reset_index()
    my_trip['order_of_point'] = my_trip.index + 1
    ordered_route_to_stopid = pd.concat([ordered_route_to_stopid, my_trip])

# get stop longtitude and latitude: x,y information for stop id 
my_data_to_xy= pd.merge(ordered_route_to_stopid, stop, how='left', on=['stop_id'])

# get route names 
my_data_final= pd.merge(my_data_to_xy, route, how='left', on=['route_id'])

# get stop time frequency / how many trips go through per stop / traffic
stop_time_groupby = stop_time.groupby(['stop_id']).count()
stop_time_groupby['stop_id'] = stop_time_groupby.index
stop_time_groupby_dict = dict(zip(stop_time_groupby['stop_id'], stop_time_groupby['trip_id']))
my_data_final['stop_time_frequency'] = my_data_final['stop_id'].map(stop_time_groupby_dict)

my_data_final['year'] = year

# all information I need for the tableau mapping 
tableau_data = my_data_final[['route_desc', 'route_id', 'route_short_name', 'route_type', 
                              'trip_id', 'order_of_point', 'stop_name', 'agency_id',
                              'stop_lat', 'stop_lon', 'stop_time_frequency', 
                              'year', 'departure_time', 'arrival_time'
                            ]]


tableau_data.to_csv('metro_route_stop_' + str(year) + '.csv')
print 'end'


# merge years data into a master database 
data_2014= pd.read_csv('metro_route_stop_2014.csv')
data_2015= pd.read_csv('metro_route_stop_2015.csv')
data_2016= pd.read_csv('metro_route_stop_2016.csv')
data_2017= pd.read_csv('metro_route_stop_2017.csv')
data_2018= pd.read_csv('metro_route_stop_2018.csv')
data = data_2014.append(data_2015)
data = data.append(data_2016)
data = data.append(data_2017)
data = data.append(data_2018)
data.to_csv('metro_route_stop_5year.csv')