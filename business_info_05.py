# -*- coding: utf-8 -*-
"""
Created on Sat Feb 24 13:43:40 2018

@author: Administrator
"""

#%%
import os
import sys
import time
import datetime as dt

today = dt.datetime.now().strftime('%Y%m%d')

import pandas as pd
import numpy as np

#%%
pyfile_folder = r'D:\XH\Python_Project\Proj_2\files'
data_folder = r'D:\XH\Python_Project\Proj_2\data'
result_folder = r'D:\XH\Python_Project\Proj_2\result'

os.chdir(pyfile_folder)
sys.path.append(pyfile_folder)

from Tookits import specific_func  
from Tookits import cal_func

#%% 导入数据
folder_name = 'good_table'
file_name = 'company_base_business_merge_new.csv'
path = os.path.join(data_folder + '\\' + folder_name,file_name)
encode = specific_func.get_txt_encode(path)
bus_info = pd.read_csv(path, encoding = encode)

if not os.path.exists(result_folder + '\\' + folder_name + '\\' + today):
    os.makedirs(result_folder + '\\' + folder_name + '\\' + today) 
#%% 以company_name为准，挑出重复数据
repeated_record = bus_info[bus_info.duplicated(subset = 'company_name', keep = False)].sort_values(by = 'company_name')

bus_info_duplicate = bus_info.drop_duplicates()
repeated_record_duplicate = bus_info_duplicate[bus_info_duplicate.duplicated(subset = 'company_name', keep = False)].sort_values(by = 'company_name')

save_filename = os.path.join(result_folder + '\\' + folder_name + '\\' + today, 'business_info_duplicate_data.xlsx')
with pd.ExcelWriter(save_filename) as writer:
    repeated_record.to_excel(writer,'未先去重')
    repeated_record_duplicate.to_excel(writer,'已先去重（所有字段相同）')
    writer.save() 

#%%
vip_fea = ['chanle_id', 'company_name', 'company_legal_name',
       'company_regis_capital', 'company_currency',
       'company_regis_code', 'company_credit_code', 
       'company_organization_code','company_country_rating',
       'company_registration_time', 'gather_time', 
       'company_industry', 'company_industry_code', 'company_area_code', 
       'company_operat_state','company_type', 
       'company_address','company_business_scope']
bus_info_data = bus_info[vip_fea]

#
def handle_punc(x):
    x = str(x)
    if len(x) == 1:
        if (x == '-') | (x == '***'):
            return np.nan
        else :
            print(x)
            return x
    else :
        return x

punc_list = ['company_country_rating','company_organization_code',
             'company_registration_time','company_address',
             'company_business_scope','company_industry','company_operat_state',
             'company_type']
for col in punc_list:
    bus_info_data[col] = bus_info_data[col].apply(handle_punc)
    
bus_info_data = bus_info_data.dropna(how = 'any')
#%% '正常','在营','存续','开业','在业'
state_list = ['正常','在营','存续','开业','在业']
def decide_state(x):
    state_result = []
    for state in state_list:
        if state in x:state_result.append(True)
    if state_result:
        return True
    else :
        return False
bus_info_data_state = bus_info_data[bus_info_data['company_operat_state'].apply(decide_state)]
    
#%% 时间  
bus_info_data_state['gather_time'] = pd.to_datetime(bus_info_data_state['gather_time'])
bus_info_data_state['company_registration_time'] = pd.to_datetime(bus_info_data_state['company_registration_time'])

# 以company_name为准，去重
bus_info_data_state = bus_info_data_state.sort_values(by = 'gather_time').\
                        drop_duplicates(subset = 'company_name', keep = 'last')

#%% 衍生变量
bus_info_data_state['exist_days'] = bus_info_data_state['gather_time'] - bus_info_data_state['company_registration_time']
bus_info_data_state['exist_days'] = bus_info_data_state['exist_days'].apply(lambda x:x.days)
#bus_info_data_state['exist_months'] = bus_info_data_state['exist_days'].apply(lambda x:x/np.timedelta64(1*60*60*24*30, 's'))
#bus_info_data_state[['gather_time','company_registration_time','exist_months']].head()

#%% 统计
fea_filename = os.path.join(result_folder + '\\' + folder_name + '\\' + today, os.path.splitext(file_name)[0] + '_desc.xlsx')
single_fea_desc = cal_func.describe(bus_info_data_state,fea_filename, data_rate = 0.1)


#%%


#administrative_area_code


























