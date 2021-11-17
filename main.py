import pandas as pd
import configparser
import os
import time

config = configparser.ConfigParser() 
config.read('settings.ini') 

print('init config')
catalog_from = config['delta']['catalog_from']
catalog_to = config['delta']['catalog_to']

print('get files')
files = os.listdir(catalog_from)
full_list = [os.path.join(catalog_from, i) for i in files]
time_sorted_list = sorted(full_list, key = os.path.getmtime)

print('compute delta')
df_new = pd.read_csv(time_sorted_list[-2:][0], sep=';', skiprows=7, encoding = 'ISO-8859-1', low_memory=False)
df_last = pd.read_csv(time_sorted_list[-2:][1], sep=';', skiprows=7, encoding = 'ISO-8859-1', low_memory=False)
dff = df_new.merge(df_last,indicator = True, how='left').loc[lambda x : x['_merge']!='both']
dff.to_csv(catalog_to+'delta'+time.strftime("%Y%m%d-%H%M%S")+'.csv', sep=';')
print('done')