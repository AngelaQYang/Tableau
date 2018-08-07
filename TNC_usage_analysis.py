import pandas as pd 
import numpy as np 
import os, sys, shutil, math, h5py

hh_weight = pd.read_csv(os.path.join(os.getcwd(),r'PSRC_Weights_Delivered_06132018_hh.csv'))
hh = pd.read_csv(os.path.join(os.getcwd(), r'published_survey_2017\2017-pr2-1-household.csv'))
trip = pd.read_csv(os.path.join(os.getcwd(), r'published_survey_2017\2017-pr2-5-trip.csv'))
person = pd.read_csv(os.path.join(os.getcwd(), r'published_survey_2017\2017-pr2-2-person.csv'))

trip = pd.read_csv(r'H:\Tableau\HHSurvey2017\2017-internal1-R-data-and-codebook\2017-internal1-R-5-trip.csv')

my_trip = trip[['hhid', 'personid', 'pernum', 'tripid', 'hhgroup', 'depart_time_mam', 'arrival_time_mam', 'google_duration', 'origin_purpose', 'dest_purpose', 
                'mode_1', 'travelers_hh', 'travelers_nonhh', 'travelers_total', 'dayofweek', 'o_tract', 'd_tract', 'o_bg', 'd_bg',
                'hh_day_wt_revised', 'trip_weight_revised']]

my_hh = hh[['hhid', 'hhgroup', 'hhsize', 'vehicle_count', 'numadults', 'numworkers', 'hhincome_broad', 'car_share', 'offpark', 'streetpark', 
            'hh_wt_revised', 'hh_day_wt_revised']]

my_person = person[['hhid', 'personid', 'pernum', 'hhgroup', 'age', 'gender', 'student', 'commute_freq', 'commute_mode', 'workpass',
                'mode_freq_1', 'mode_freq_2', 'mode_freq_3', 'mode_freq_4', 'mode_freq_5', 'hh_wt_revised', 'hh_day_wt_revised']]

my_trip['TNC'] = 0 
my_trip.ix[my_trip['mode_1'] == 37, 'TNC'] = 1
my_trip['TNCtot'] = my_trip['TNC']*my_trip['trip_weight_revised']
my_trip['Transit'] = 0 
my_trip.ix[(my_trip['mode_1'] == 23) or (my_trip['mode_1'] == 23)27, 28, 32, 41, 42, 52], 'Transit'] = 1

census = pd.read_csv(r'H:\Tableau\HHSurvey2017\result\tl_2017_53_tract\census_tract.txt')
my_trip2 = pd.merge(my_trip, census, how = 'left', left_on = 'o_tract', right_on='GEOID')


