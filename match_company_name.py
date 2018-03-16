# -*- coding: utf-8 -*-
"""
Created on Thu Mar 15 17:15:49 2018

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
DB_CON_STR = 'mysql+pymysql://root:123456@localhost/standard_lib_5_mysql?charset=utf8'  
engine = create_engine(DB_CON_STR, echo=False) 

# Hive
conn = connect(host="192.168.20.102", port=10000,  # database="system", 
               auth_mechanism="PLAIN",
               user = 'admin', password = 'admin')
cursor = conn.cursor()

#%%
file_name = '17497公司名.txt'

file_path = os.path.join(data_folder,file_name)
encode = specific_func.get_txt_encode(file_path)
fid = open(file_path, encoding = encode)
total_company = fid.readlines()
fid.close()

total_company = pd.DataFrame(total_company, columns = ['company_name'])
total_company['company_name'] = total_company['company_name'].apply(lambda x: x.strip())
total_company['total_count'] = 1
total_company_unique = total_company.drop_duplicates('company_name')

#%%
database_name = 'odm_1'
cursor.execute("use "+ database_name) 

#table_name = 'odm_company_base_business_merge'    
#cursor.execute("select company_name from %s"%table_name)
#total_company = as_pandas(cursor)   
#total_company['total_count'] = 1
#total_company_unique = total_company.drop_duplicates('company_name')

table_name = 'odm_company_outbound_investment'    
cursor.execute("select company_name from %s"%table_name)
outbound_investment = as_pandas(cursor)   
outbound_investment['outbound_count'] = 2
outbound_investment_unique = outbound_investment.drop_duplicates('company_name')

table_name = 'odm_company_branch'    
cursor.execute("select father_company_name from %s"%table_name)
company_branch = as_pandas(cursor)   
company_branch['branch_count'] = 3
company_branch_unique = company_branch.drop_duplicates('father_company_name')
 
#%
print('total_company ：', total_company.shape)
print('total_company_unique ：', total_company_unique.shape)
print('outbound_investment ：', outbound_investment.shape)
print('outbound_investment_unique ：', outbound_investment_unique.shape)
print('company_branch ：', company_branch.shape)
print('company_branch_unique ：', company_branch_unique.shape)

#%%
total_outbound = pd.merge(total_company_unique, outbound_investment_unique,
                          on = 'company_name', how = 'outer')

outbound_not_in_total = total_outbound[(total_outbound['total_count'].isnull()) &
                                              (total_outbound['outbound_count'].notnull())]
total_not_in_outbound = total_outbound[(total_outbound['total_count'].notnull()) &
                                              (total_outbound['outbound_count'].isnull())]
outbound_total_same = total_outbound[(total_outbound['total_count'].notnull()) & 
                                   (total_outbound['outbound_count'].notnull())]

print('total_outbound ：', total_outbound.shape)
print('outbound_not_in_total ：', outbound_not_in_total.shape)
print('total_not_in_outbound ：', total_not_in_outbound.shape)
print('outbound_total_same ：', outbound_total_same.shape)

#%%
total_branch = pd.merge(total_company_unique, company_branch_unique,
                          left_on = 'company_name', 
                          right_on = 'father_company_name', how = 'outer')

branch_not_in_total = total_branch[(total_branch['total_count'].isnull()) &
                                              (total_branch['branch_count'].notnull())]
total_not_in_branch = total_branch[(total_branch['total_count'].notnull()) &
                                              (total_branch['branch_count'].isnull())]
branch_total_same = total_branch[(total_branch['total_count'].notnull()) & 
                                   (total_branch['branch_count'].notnull())]

print('total_branch ：', total_branch.shape)
print('branch_not_in_total ：', branch_not_in_total.shape)
print('total_not_in_branch ：', total_not_in_branch.shape)
print('branch_total_same ：', branch_total_same.shape)

#%%
branch = company_branch.groupby(['father_company_name']).count()


#%%









