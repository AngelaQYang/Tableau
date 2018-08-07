import pandas as pd
import numpy as np
import os, sys, shutil, math, h5py
os.chdir('H:\Tableau\GTFS')


feed_list = ['Amazon_GTFS_data', 'CommunityTransit_GTFS_data', 'EverettTransit_GTFS_data', 'Ferry_GTFS_data', 'KitsapTransit_GTFS_data', 'Metro_GTFS_data', 'PierceTransit_GTFS_data', 'SoundTransit_GTFS_data']


year_dict = {'2018': '/2018/', '2017': '/2017/',
             '2016': '/2016/', '2015': '/2015/',
             '2014': '/2014/'}

for feed in feed_list: 
    print feed
    year_list = [dI for dI in os.listdir(feed) if os.path.isdir(os.path.join(feed,dI))]
    print year_list
    for year in year_list:
        print year
        # get GTFS data
        route = pd.read_csv('H:/Tableau/GTFS/' + feed + year_dict[year] +'routes.txt')
        trip = pd.read_csv('H:/Tableau/GTFS/' + feed + year_dict[year] + 'trips.txt')
        stop = pd.read_csv('H:/Tableau/GTFS/' + feed + year_dict[year] + 'stops.txt')
        stop_time = pd.read_csv('H:/Tableau/GTFS/' + feed + year_dict[year] + 'stop_times.txt')
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
        tableau_data = my_data_final[['route_id', 'route_short_name', 'route_type', 
                                      'trip_id', 'order_of_point', 'stop_name', 'agency_id',
                                      'stop_lat', 'stop_lon', 'stop_time_frequency', 
                                      'year', 'departure_time', 'arrival_time'
                                      ]]
        tableau_data.to_csv(feed + '_route_stop_' + str(year) + '.csv')
        
        print feed, year, 'end-----------'

    # merge historical years and 2018 years 
    data_2018= pd.read_csv(feed + '_route_stop_2018.csv')
    for y in year_list:
        print y 
        if y != '2018':
            print y
            data_historic_year = pd.read_csv(feed + '_route_stop_' + y + '.csv')
            np.unique(data_historic_year['year'])
            data_2018 = data_2018.append(data_historic_year)
        if y == '2018':
            print y
            continue
        print np.unique(data_2018['year'])
    my_data = data_2018
    my_data = my_data.to_csv(feed + '_route_stop_5year.csv')
    
    print feed, 'end*******************************************'



# merge with everyone
data_list = [dI for dI in os.listdir('outputs') if os.path.isdir(os.path.join('outputs',dI))]
my_data = pd.read_csv('Amazon_GTFS_data_route_stop_5year.csv')
my_data = my_data[:0]
for feed in feed_list:
    print feed
    file_name = feed + '_route_stop_5year.csv'
    feed_data = pd.read_csv(file_name)
    my_data = my_data.append(feed_data)

my_data.to_csv('outputs/5years/total_feed_5_years.csv')









