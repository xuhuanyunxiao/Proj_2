# -*- coding: utf-8 -*-
"""
Created on Tue Mar  6 09:58:09 2018

@author: Administrator
"""
#%%
import os
import sys
import datetime as dt
import pandas as pd

today = dt.datetime.now().strftime('%Y%m%d')

from sqlalchemy import create_engine
from pandas.io import sql

from impala.dbapi import connect

names = locals()

#
pyfile_folder = r'D:\XH\Python_Project\Proj_2\files'
data_folder = r'D:\XH\Python_Project\Proj_2\data\ETL_data'
result_folder = r'D:\XH\Python_Project\Proj_2\result\ETL_result'

os.chdir(pyfile_folder)
sys.path.append(pyfile_folder)

#%% 建立连接
# MySQL
DB_CON_STR = 'mysql+pymysql://root:123456@localhost/standard_lib_5_mysql?charset=utf8'  
engine = create_engine(DB_CON_STR, echo=False) 

# Hive
conn = connect(host="192.168.20.102", port=10000,  # database="system", 
               auth_mechanism="PLAIN",
               user = 'admin', password = 'admin')
cursor = conn.cursor()

#%% 标准表： standard_lib_5_mysql
folder_name = database_name = 'standard_lib_5'
save_filename = os.path.join(result_folder, 
                             'standard_lib_5\\hadoop语句_standard_lib_' + today + '.txt')
file = open(save_filename,"w")

table_list = [list(x)[0] for x in pd.read_sql_query('show tables', engine).values]

cursor.execute("create database if not exists {0} ".format(database_name))
cursor.execute("use "+ database_name)
for table_name in table_list:
    # 读入MySQL上的标准表数据，并写入csv文件    
    names['%s' %table_name] = sql.read_sql(table_name, engine).drop('index', axis = 1)
    file_name = result_folder +  '\\standard_lib_5\\' + table_name + '.csv'
    names['%s' %table_name].to_csv(file_name, index = False, 
                                     header = False, encoding = 'utf-8')
    field = [x + ' string' for x in names['%s' %table_name].columns.tolist()]
    
    # 在hive上建立标准表 
    cursor.execute('drop table if exists %s;' %table_name)     
    sql_code  =  "create external table if not exists {0}{1}".\
            format(table_name,tuple(field)).replace("'","") \
            + '\n' + "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','" \
            + '\n' + "LOCATION '/tmp/20180315/{0}'".format(table_name)      
    cursor.execute(sql_code)
    
    # 打印在hadoop上操作的语句
#    file.write("load data inpath '/home/hadoop/Public/ETL_data/{0}/{1}.csv' \ # hive 上用
#               into table {1};".format(database_name,table_n) + "\n")
    file.write("hdfs dfs -put -f '/home/hadoop/Public/ETL_data/{0}/{1}.csv' '/tmp/20180315/{1}'".\
               format(database_name,table_name) + "\n")    
# 如果已经put过，需加参数 -f
#hdfs dfs -put -f '/home/hadoop/Public/ETL_data/standard_lib/city_symbol.csv' '/tmp/20180302/city_symbol'
    
file.close()

#%%
