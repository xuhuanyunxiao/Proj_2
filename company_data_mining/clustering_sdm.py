# -*- coding: utf-8 -*-
"""
Created on Mon Mar 19 09:59:58 2018

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

#%% 建立连接
# MySQL
DB_CON_STR = 'mysql+pymysql://root:123456@localhost/sdm_2_mysql?charset=utf8'  
engine = create_engine(DB_CON_STR, echo=False) 

# Hive
conn = connect(host="192.168.20.102", port=10000,  # database="system", 
               auth_mechanism="PLAIN",
               user = 'admin', password = 'admin')
cursor = conn.cursor()

#%% 
# hive
#database_name = 'sdm_2'
#cursor.execute("use "+ database_name) 
#
#table_name = 'sdm_company_base_business_merge'    
#cursor.execute("select * from %s"%table_name)
#bussiness_info = as_pandas(cursor)   
##bussiness_info = cursor.fetchall()

# mysql
business_info = pd.read_sql_table('sdm_company_base_business_merge', engine)
if 'index' in business_info.columns.tolist():
    business_info = business_info.drop(['index'], axis = 1)

#%%
from sklearn.cluster import KMeans
from sklearn.datasets import make_blobs

random_state = 170

X = business_info[['company_regis_capital','exist_days',]]
y_pred = KMeans(n_clusters=5, random_state=random_state).fit_predict(X)



#%%
import matplotlib.pyplot as plt  

plt.figure(figsize=(16,20))  
colors = ['b', 'g', 'r']  
markers = ['o', 's', 'D']  
for i,l in enumerate(y_pred[:2000]):  
     plt.plot(X['company_regis_capital'][i],X['exist_days'][i],
              color=colors[l],marker=markers[l],ls='None')  
plt.show() 

#%%




