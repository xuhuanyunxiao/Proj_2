# -*- coding: utf-8 -*-
"""
Created on Wed Mar 14 10:37:47 2018

@author: Administrator
"""
#%%
import os
import datetime as dt

today = dt.datetime.now().strftime('%Y%m%d')

import pandas as pd
import numpy as np

from impala.dbapi import connect
from impala.util import as_pandas

from sqlalchemy import create_engine
from pandas.io import sql

#
pyfile_folder = r'D:\XH\Python_Project\Proj_2\files'
data_folder = r'D:\XH\Python_Project\Proj_2\data\ETL_data'
result_folder = r'D:\XH\Python_Project\Proj_2\result\ETL_result'

os.chdir(pyfile_folder)

#%% 建立连接
# MySQL
DB_CON_STR = 'mysql+pymysql://root:123456@localhost/odm_1_mysql?charset=utf8'  
engine = create_engine(DB_CON_STR, echo=False) 

# Hive
conn = connect(host="192.168.20.102", port=10000,  # database="system", 
               auth_mechanism="PLAIN",
               user = 'admin', password = 'admin')
cursor = conn.cursor()

#%%  每张表中挑选的字段
file_path = data_folder + '\\4-1 数据仓库（53张表+标准表）.xlsx'
table_col_info = pd.read_excel(file_path, '数据处理', header = 2)
table_col_info['index'] = np.arange(table_col_info.shape[0])
table_col_info.set_index('index', inplace = True)
table_col_info_fillna = table_col_info[['一级分类', '二级分类','数据表名']]
table_col_info_fillna.fillna(method = 'ffill', inplace = True)
table_col_info.update(table_col_info_fillna)
table_col_info['table_name'] = table_col_info['数据表名'].apply(lambda x: x.split('-')[1])

#% 有些表中是字段 gather_time ，有些是 company_gather_time
database_name = 'data_hub_new'
cursor.execute("use "+ database_name) 

company_gather_list = []
gather_list = []
no_gather_list = []

company_name_list = []
no_name_list = []

for table_name in table_col_info['table_name'].unique():
    try: 
        cursor.execute("select * from %s limit 5"%table_name)
    except :
        table_name = table_name + '_new'
        cursor.execute("select * from %s limit 5"%table_name)
    tmp_data = as_pandas(cursor)
    if 'gather_time' in tmp_data.columns.tolist():
        gather_list.append(table_name)
    elif 'company_gather_time' in tmp_data.columns.tolist():
        company_gather_list.append(table_name)
    else :
        no_gather_list.append(table_name)
        
    if 'company_name' in tmp_data.columns.tolist():
        company_name_list.append(table_name)
    else :
        no_name_list.append(table_name)
        
#%  全量
folder_name = 'HiveSQL' 
count = 1
num = 0

save_filename = os.path.join(result_folder + '\\' + folder_name, 'ODM_1_all_' + today + '.txt')
file = open(save_filename,"w")
file.write('USE odm_1;' + "\n" *2)

#%
for table_name in table_col_info['table_name'].unique():
    col_list = []
    col_list = table_col_info[table_col_info['table_name'] == table_name]['三级分类（英文名）'].tolist()
    CN_name = table_col_info[table_col_info['table_name'] == table_name]['二级分类'].unique().tolist()

    try: 
        cursor.execute("select * from %s limit 5"%table_name)
    except :
        table_name = table_name + '_new'
        cursor.execute("select * from %s limit 5"%table_name)
    
    if 'chanle_id' not in col_list:col_list.append('chanle_id')
#    if table_name == 'company_base_business_merge_new' :
#        if 'company_name' not in col_list:col_list.append('company_name')
    if table_name in company_name_list:
        if 'company_name' not in col_list:col_list.append('company_name')
    if table_name in company_gather_list:
        if 'company_gather_time' not in col_list:col_list.append('company_gather_time')
    elif table_name in gather_list:
        if 'gather_time' not in col_list:col_list.append('gather_time')
        
    new_table_name = 'odm_' + table_name.replace('_new', '')
    num += 1
    file.write('-- ----  %s %s;' %(str(num), CN_name[0]) + "\n")                
    file.write('drop table if exists %s;' %new_table_name + "\n")
    file.write('create table `%s` ' %new_table_name  + "\n") 
    file.write('AS'  + "\n")
    file.write('select '  + "\n")
#    file.write("select %s" %', '.join(col_list)  + "\n")    
    for index, col in enumerate(col_list):
        if index == (len(col_list) - 1):
            file.write('\t' * count + '%s ' %col + "\n")
        else :
            file.write('\t' * count + '%s, ' %col + "\n")

    file.write("from data_hub_new.%s ;" %table_name + "\n" * 3)
    
file.close()
    
#%% 增量


#%% 全量  ODM to MySQL
def run_hive_query(sql_command):   
    cursor.execute(sql_command)  
    return cursor.fetchall() 

database_name = 'odm_1'
cursor.execute("use "+ database_name) 

table_list = [name[0] for name in run_hive_query("show tables")] 

for table_name in table_list:
    cursor.execute("select * from %s"%table_name)
    tmp_data = as_pandas(cursor)
    sql.to_sql(tmp_data, table_name, 
           engine, schema='odm_1_mysql', if_exists='replace') 

#%%


