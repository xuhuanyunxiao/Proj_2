# -*- coding: utf-8 -*-
"""
Created on Tue Mar  6 09:58:09 2018

@author: Administrator
"""
#%%
import os
import sys
import datetime as dt

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
DB_CON_STR = 'mysql+pymysql://root:123456@localhost/standard_lib_mysql?charset=utf8'  
engine = create_engine(DB_CON_STR, echo=False) 

# Hive
conn = connect(host="192.168.20.102", port=10000,  # database="system", 
               auth_mechanism="PLAIN",
               user = 'admin', password = 'admin')
cursor = conn.cursor()

#%% 标准表：standard_lib
save_filename = result_folder + '\\hadoop语句_standard_lib_' + today + '.txt' 
file = open(save_filename,"w")
database_name = 'standard_lib'
table_name = ['city_symbol','prov_dist_county_symbol',
              'economic_category_2017','economic_category_2011']

cursor.execute("create database if not exists {0} ".format(database_name))
cursor.execute("use "+ database_name)
for table_n in table_name:
    # 读入MySQL上的标准表数据，并写入csv文件
    file_name = result_folder +  '\\standard_lib\\' + table_n + '.csv'
    names['%s' %table_n] = sql.read_sql(table_n, engine).drop('index', axis = 1)
    names['%s' %table_n].to_csv(file_name, index = False, encoding = 'utf-8')

    # 在hive上建立标准表 
    cursor.execute('drop table if exists %s;' %table_n)
    field = [x + ' string' for x in names['%s' %table_n].columns.tolist()] 
    sql_code  =  "create external table if not exists {0}{1}".\
            format(table_n,tuple(field)).replace("'","") \
            + '\n' + "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','" \
            + '\n' + "LOCATION '/tmp/20180302/{0}'".format(table_n)      
    cursor.execute(sql_code)
    
    # 打印在hadoop上操作的语句
#    file.write("load data inpath '/home/hadoop/Public/ETL_data/{0}/{1}.csv' \ # hive 上用
#               into table {1};".format(database_name,table_n) + "\n")
    file.write("hdfs dfs -put '/home/hadoop/Public/ETL_data/{0}/{1}.csv' '/tmp/20180302/{1}'".\
               format(database_name,table_n) + "\n")    
# 如果已经put过，需加参数 -f
#hdfs dfs -put -f '/home/hadoop/Public/ETL_data/standard_lib/city_symbol.csv' '/tmp/20180302/city_symbol'
    
file.close()

#%%
