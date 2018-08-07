import pandas as pd
import numpy as np
import os

data = pd.read_csv(os.path.join(os.getcwd(), r'2016permitdata\REG1016PMT.csv'))

#clean data
data = data[(data['PS'] != 1) & (data['PS'] != 2) & (data['PS'] != 4)]
data = data[(data['TYPE4'] != 'GQ') & (data['TYPE4'] != 'TL') & (data['TYPE4'] != '???') & (data['TYPE4'] != '')]

data.to_csv('10_16PMT.csv')
