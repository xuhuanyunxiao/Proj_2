# -*- coding: utf-8 -*-
"""
Created on Tue Mar 13 10:29:57 2018

@author: Administrator
"""

#%%
import os
import re
import sys
import datetime as dt

today = dt.datetime.now().strftime('%Y%m%d')
names = locals()

import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.colors as colors
%matplotlib inline

from sqlalchemy import create_engine
from pandas.io import sql

from impala.dbapi import connect
from impala.util import as_pandas

pyfile_folder = r'D:\XH\Python_Project\Proj_2\files'
data_folder = r'D:\XH\Python_Project\Proj_2\data\ETL_data'
result_folder = r'D:\XH\Python_Project\Proj_2\result\ETL_result'

os.chdir(pyfile_folder)
sys.path.append(pyfile_folder)

from Tookits import specific_func  
from Tookits import cal_func

specific_func.set_ch()

#%%
config_file = data_folder + '\\数据流向 3 FDM.xlsx'
config_data = pd.read_excel(config_file)
config_data['sdm_表名'].fillna(method = 'ffill', inplace = True)
config_data['是否先去重'].fillna(method = 'ffill', inplace = True)
configs = config_data[['fdm_字段名', 'fdm_字段解释', '是否先去重', 'sdm_表名']]
print(configs.shape)
configs.tail()

#%%
table_list = configs[['是否先去重','sdm_表名']].drop_duplicates()
print(configs['sdm_表名'].unique().shape)
table_list

#%%
credit_scroe = pd.DataFrame()
num = 0
for [flag, sdm_table_name] in list(table_list.values):
    num +=1
    print('* 读取第 %s 张表：'%str(num), sdm_table_name)
    fields = configs[configs['sdm_表名'] == sdm_table_name]['fdm_字段名'].tolist()
    if 'company_name' not in fields:fields.append('company_name')
    #if 'distinct_name' in fields:fields.remove('distinct_name')
    
    table_path = result_folder +  '\\sdm_2\\csv_data\\' + sdm_table_name + '.csv'
    tmp_data = pd.read_csv(table_path, sep='^')
    names['%s'%sdm_table_name] = tmp_data[fields]
    if flag == 1:
        names['%s'%sdm_table_name] = names['%s'%sdm_table_name].drop_duplicates()
    
    if num == 1:
        to_add_num = 0
        credit_scroe = names['%s'%sdm_table_name]
    else :
        to_add_num = credit_scroe.shape[0]
        credit_scroe = pd.merge(credit_scroe, names['%s'%sdm_table_name],
                               on = 'company_name', how = 'left')
    
    print('    -- 数据维度：', names['%s'%sdm_table_name].shape)
    print('    -- 融合后数据：维度', credit_scroe.shape, 
          '  增加量：', credit_scroe.shape[0] - to_add_num)
    
    del tmp_data

#%%
import os  
import time  
  
def mkSubFile(lines,head,srcName,sub):  
    [des_filename, extname] = os.path.splitext(srcName)  
    filename  = des_filename + '_' + str(sub) + extname  
    print( 'make file: %s' %filename)  
    fout = open(filename,'w', encoding='utf-8')  
    try:  
        fout.writelines([head])  
        fout.writelines(lines)  
        return sub + 1  
    finally:  
        fout.close()  
  
def splitByLineCount(filename,count):  
    fin = open(filename,'r', encoding='utf-8')  
    try:  
        head = fin.readline()  
        buf = []  
        sub = 1  
        for line in fin:  
            buf.append(line)  
            if len(buf) == count:  
                sub = mkSubFile(buf,head,filename,sub)  
                buf = []  
        if len(buf) != 0:  
            sub = mkSubFile(buf,head,filename,sub)     
    finally:  
        fin.close()  

#%%   
file_path = r'D:\XH\Python_Project\Proj_2\data\ETL_data\new_data\base_business.csv'    
    
begin = time.time()  
splitByLineCount(file_path,600000)  
end = time.time()  
print('time is %d seconds ' % (end - begin)) 

#%%


import xgboost as xgb

xgb.plot_importance()










