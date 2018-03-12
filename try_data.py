# -*- coding: utf-8 -*-
"""
Created on Fri Mar  9 11:45:21 2018

@author: Administrator
"""
#%%
import os
import pandas as pd
names = locals()

#
pyfile_folder = r'D:\XH\Python_Project\Proj_2\files'
data_folder = r'D:\XH\Python_Project\Proj_2\data\try_data'
result_folder = r'D:\XH\Python_Project\Proj_2\result\ETL_result'

os.chdir(pyfile_folder)

#%% 建立连接
file_list = os.listdir(data_folder)
combined_data = pd.DataFrame()
for file_name in file_list:
    file = os.path.splitext(file_name)[0]
    names['%s' %file] = pd.read_csv(os.path.join(data_folder, file_name),sep = '^')
    combined_data = pd.concat([combined_data, names['%s' %file]])


#%%



