# -*- coding: utf-8 -*-
"""
Created on Wed Mar 14 15:05:22 2018

@author: Administrator
"""

#%%
import re
import os
import sys
import datetime as dt

today = dt.datetime.now().strftime('%Y%m%d')

import pandas as pd
import numpy as np

from sqlalchemy import create_engine
from pandas.io import sql

from impala.dbapi import connect
from impala.util import as_pandas

names = locals()

#
pyfile_folder = r'D:\XH\Python_Project\Proj_2\files'
data_folder = r'D:\XH\Python_Project\Proj_2\data\ETL_data'
result_folder = r'D:\XH\Python_Project\Proj_2\result\ETL_result'

os.chdir(pyfile_folder)
sys.path.append(pyfile_folder)

from Tookits import specific_func  
from Tookits import cal_func

#%%
runfile('D:/XH/Python_Project/Proj_2/files/set_environment.py')

#% 结果文件夹结构
database_list = ['odm_1','sdm_2','fdm_3','gdm_4',
                 'standard_lib_5','supplemental_lib_6']
for folder_n in database_list:
    if not os.path.exists(result_folder + '\\' + folder_n):
        os.mkdir(result_folder + '\\' + folder_n)
        
#%% 建立连接
# MySQL
DB_CON_STR = 'mysql+pymysql://root:123456@localhost/supplemental_lib_6_mysql?charset=utf8'  
engine = create_engine(DB_CON_STR, echo=False) 

# Hive
conn = connect(host="192.168.20.102", port=10000,  # database="system", 
               auth_mechanism="PLAIN",
               user = 'admin', password = 'admin')
cursor = conn.cursor()
#%%  全量
folder_name = database_name = 'supplemental_lib_6'
file_list = os.listdir(os.path.join(data_folder, folder_name))

if not os.path.exists(result_folder + '\\' + folder_name + '\\' + today):
    os.makedirs(result_folder + '\\' + folder_name + '\\' + today) 
    
save_filename = os.path.join(result_folder, 
                             'supplemental_lib_6\\hadoop语句_supplemental_lib_' + today + '.txt')
file = open(save_filename,"w")

cursor.execute("create database if not exists {0} ".format(database_name))
cursor.execute("use "+ database_name)

for file_name in file_list:
    table_name = os.path.splitext(file_name)[0]
    names['%s' %table_name] = pd.read_csv(os.path.join(data_folder + '\\' + folder_name, file_name), 
                           encoding = 'ANSI')
    file_path = result_folder +  '\\supplemental_lib_6\\' + table_name + '.csv'
    names['%s' %table_name].to_csv(file_path, index = False, 
                                   header = False,  encoding = 'utf-8')
    # 统计
    fea_filename = os.path.join(result_folder + '\\' + folder_name + '\\' + today, table_name + '.xlsx')        
    single_fea_desc = cal_func.describe(names['%s' %table_name],fea_filename, data_rate = 0.1)
        
    field = [x.strip() + ' string' 
             if ind > 0 else 'index string'
             for ind, x in enumerate(names['%s' %table_name].columns.tolist())]
    # 在hive上建立标准表 
    cursor.execute('drop table if exists %s;' %table_name)     
    sql_code  =  "create external table if not exists {0}{1}".\
            format(table_name,tuple(field)).replace("'","") \
            + '\n' + "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','" \
            + '\n' + "LOCATION '/tmp/20180314/{0}'".format(table_name)      
    cursor.execute(sql_code)
    
    # 写入mysql
#    sql.to_sql(names['%s' %table_name], table_name, 
#       engine, schema='supplemental_lib_6_mysql', if_exists='replace') 
        
    # 打印在hadoop上操作的语句
#    file.write("load data inpath '/home/hadoop/Public/ETL_data/{0}/{1}.csv' \ # hive 上用
#               into table {1};".format(database_name,table_n) + "\n")
    file.write("hdfs dfs -put -f '/home/hadoop/Public/ETL_data/{0}/{1}.csv' '/tmp/20180314/{1}'".\
               format(database_name,table_name) + "\n")    
# 如果已经put过，需加参数 -f
#hdfs dfs -put -f '/home/hadoop/Public/ETL_data/standard_lib/city_symbol.csv' '/tmp/20180302/city_symbol'
    
file.close()    
    
#%% 增量


#%% 全量  ODM to MySQL
def run_hive_query(sql_command):   
    cursor.execute(sql_command)  
    return cursor.fetchall() 

database_name = 'supplemental_lib_6'
cursor.execute("use "+ database_name) 

table_list = [name[0] for name in run_hive_query("show tables")] 

for table_name in table_list:
    cursor.execute("select * from %s"%table_name)
    tmp_data = as_pandas(cursor)
    sql.to_sql(tmp_data, table_name, 
           engine, schema='supplemental_lib_6_mysql', if_exists='replace') 
    
#%%
def run_hive_query(sql_command):   
    cursor.execute(sql_command)  
    return cursor.fetchall() 
    
database_name = 'data_hub_new'
cursor.execute("use "+ database_name) 

table_name = 'company_base_business_merge_new'    
cursor.execute("select company_name from %s"%table_name)
business_info = as_pandas(cursor)        
    
database_name = 'supplemental_lib_6'
cursor.execute("use "+ database_name) 
    
table_list = [name[0] for name in run_hive_query("show tables")] 
    
for table_n in table_list:
    if 'relation' in table_n:
        cursor.execute("select * from %s"%table_n)
        names['%s' %table_n] = as_pandas(cursor)  
    else :
        cursor.execute("select * from %s"%table_n)
        names['%s' %table_n] = as_pandas(cursor)  
    
#%%
business_info_unique = business_info.drop_duplicates('company_name')  

relation = pd.concat([relation1, relation2], axis = 0)

y_dbfr = pd.concat([ydb, yfr], axis = 0)
y_dbfrgd = pd.concat([y_dbfr, ygd], axis = 0)
y_dbfrgdgl = pd.concat([y_dbfrgd, ygl], axis = 0)

for table_name in ['relation', 'y_dbfrgdgl']:
    # 统计
    fea_filename = os.path.join(result_folder + '\\' + folder_name + '\\' + today, table_name + '.xlsx')        
    single_fea_desc = cal_func.describe(names['%s' %table_name],fea_filename, data_rate = 0.1)
    
#%%
relation_name = pd.DataFrame(relation['from_member_name'].tolist() + 
                             relation['to_member_name'].tolist(), 
                             columns = ['company_name'])
relation_name_unique = relation_name['company_name'].drop_duplicates()
relation_name_unique = pd.DataFrame(relation_name_unique)
info_relation = pd.merge(business_info_unique, relation_name_unique,
                    on = 'company_name', how = 'inner')
    
y_dbfrgdgl_name = pd.DataFrame(y_dbfrgdgl['cust_name'].tolist() + 
                             y_dbfrgdgl['rel_cust_cert_name'].tolist(), 
                             columns = ['company_name'])
y_dbfrgdgl_name_unique = y_dbfrgdgl_name['company_name'].drop_duplicates()
y_dbfrgdgl_name_unique = pd.DataFrame(y_dbfrgdgl_name_unique)
info_y = pd.merge(business_info_unique, y_dbfrgdgl_name_unique,
                    on = 'company_name', how = 'inner')
    
relation_y = pd.concat([relation_name,y_dbfrgdgl_name], axis = 0)
relation_y_unique = relation_y['company_name'].drop_duplicates()
relation_y_unique = pd.DataFrame(relation_y_unique)
info_relation_y = pd.merge(business_info_unique, relation_y_unique,
                    on = 'company_name', how = 'inner')

#%%    
print('business_info ：',business_info.shape)    
print('business_info_unique ：',business_info_unique.shape)
    
print('relation ：',relation.shape)    
print('relation_name ：',relation_name.shape) 
print('relation_name_unique ：',relation_name_unique.shape) 
   
print('y_dbfrgdgl ：',y_dbfrgdgl.shape)    
print('y_dbfrgdgl_name ：',y_dbfrgdgl_name.shape)    
print('y_dbfrgdgl_name_unique ：',y_dbfrgdgl_name_unique.shape)    

print('info_relation ：',info_relation.shape)    
print('info_y ：',info_y.shape)    
print('info_relation_y ：',info_relation_y.shape)    
    
#%%
relation_unique_from = relation.drop_duplicates('from_member_name')
relation_unique_to = relation.drop_duplicates('to_member_name')

info_relation_from = pd.merge(business_info_unique, relation_unique_from,
                    left_on = 'company_name', right_on = 'from_member_name',
                    how = 'inner')

info_relation_to = pd.merge(business_info_unique, relation_unique_to,
                    left_on = 'company_name', right_on = 'to_member_name',
                    how = 'inner')

y_dbfrgdgl_unique_cust = y_dbfrgdgl.drop_duplicates('cust_name')    
y_dbfrgdgl_unique_rel_cust = y_dbfrgdgl.drop_duplicates('rel_cust_cert_name')    

info_y_dbfrgdgl_cust = pd.merge(business_info_unique, y_dbfrgdgl_unique_cust,
                    left_on = 'company_name', right_on = 'cust_name',
                    how = 'inner')

info_y_dbfrgdgl_rel_cust = pd.merge(business_info_unique, y_dbfrgdgl_unique_rel_cust,
                    left_on = 'company_name', right_on = 'rel_cust_cert_name',
                    how = 'inner')


print('relation_unique_from ：',relation_unique_from.shape)    
print('relation_unique_to ：',relation_unique_to.shape)    
print('info_relation_from ：',info_relation_from.shape)    
print('info_relation_to ：',info_relation_to.shape)  
  
print('y_dbfrgdgl_unique_cust ：',y_dbfrgdgl_unique_cust.shape)    
print('y_dbfrgdgl_unique_rel_cust ：',y_dbfrgdgl_unique_rel_cust.shape)    
print('info_y_dbfrgdgl_cust ：',info_y_dbfrgdgl_cust.shape)    
print('info_y_dbfrgdgl_rel_cust ：',info_y_dbfrgdgl_rel_cust.shape)    


#%%    
    