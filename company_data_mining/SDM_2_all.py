# -*- coding: utf-8 -*-
"""
Created on Wed Mar 14 14:07:20 2018

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

#%% 建立连接
# MySQL
DB_CON_STR = 'mysql+pymysql://root:123456@localhost/standard_lib_mysql?charset=utf8'  
engine = create_engine(DB_CON_STR, echo=False) 

# Hive
conn = connect(host="192.168.20.102", port=10000,  # database="system", 
               auth_mechanism="PLAIN",
               user = 'admin', password = 'admin')
cursor = conn.cursor()

#%% company_base_business_merge_new（企业工商注册）
# 从hive上取数据
database_name = 'odm_1'
cursor.execute("use "+ database_name) 

table_name = 'odm_company_base_business_merge'
cursor.execute("select * from %s"%table_name)
business_info = as_pandas(cursor)
#business_info = busines_info.drop([0], axis = 0)
if business_info.iloc[0,0] == business_info.columns.tolist()[0]:
    business_info = business_info.drop(0, axis = 0)

ETL_business_info = business_info.copy()
data_size = [] # 每个阶段的数据量及特征量
data_size.append(["0 原始数据", ETL_business_info.shape])

#%
# 'company_name','chanle_id' 均不为空
ETL_business_info = ETL_business_info[ETL_business_info['company_name'].notnull() & 
                                      ETL_business_info['chanle_id'].notnull()]
data_size.append(["1 公司名、id、公司状态 均不为空", ETL_business_info.shape])

# 处理 ‘’、‘-’、‘***’等情况
def handle_punc(x):
    x = str(x)
    if (len(x) == 1) & (x == '-'):
        return 'EEEEE'
    elif (len(x) == 3) & (x == '***'):
        return 'EEEEE'
    else :
        if x == '未公开':
            return 'EEEEE'
        else :
            return x

ETL_business_info = ETL_business_info.applymap(handle_punc)
ETL_business_info = ETL_business_info.applymap(lambda x: x.replace('', 'EEEEE') if len(x) == 0 else x)

#% 转换时间
ETL_business_info['gather_time'] = pd.to_datetime(ETL_business_info['gather_time'])

ETL_business_info['company_registration_time'] = ETL_business_info['company_registration_time'].replace('EEEEE',np.nan)
def get_correct_date(x):
    mat = re.search(r"(\d{4}年\d{1,2}月\d{1,2}日)",x)
    try :
        if mat:
            date  = mat.groups(0)[0].replace('年','/').replace('月','/').replace('日','/')
            return date
        else :
            print('-- 无匹配 --')
            print('---', x)
            return np.nan
    except :
        return np.nan
    
for index in ETL_business_info['company_registration_time'].index:
    x  = ETL_business_info['company_registration_time'][index]
    try :
        pd.to_datetime(x)
    except Exception as e :
        print(e)
        print(x)
        print(index)
        ETL_business_info['company_registration_time'][index] = get_correct_date(x)

ETL_business_info['company_registration_time'] = pd.to_datetime(ETL_business_info['company_registration_time'])

# 衍生变量：公司存在时间
ETL_business_info['exist_days'] = ETL_business_info['gather_time'] - ETL_business_info['company_registration_time']
ETL_business_info['exist_days'] = ETL_business_info['exist_days'].apply(lambda x:x.days)
data_size.append(["3 增加衍生变量 公司存在时间（天）", ETL_business_info.shape])

#%
# 先按gather_time排序，后面去重取最新数据
ETL_business_info = ETL_business_info.sort_values(by = 'gather_time', 
                                                  ascending = False, 
                                                  na_position = 'first')
## 去重
### 所有字段均重复
ETL_business_info = ETL_business_info[~ETL_business_info.duplicated()] 
data_size.append(["6 所有字段均重复", ETL_business_info.shape])

### 所有 ['company_name','chanle_id'] 均重复。
ETL_business_info = ETL_business_info[~ETL_business_info.duplicated(['company_name','chanle_id'])] 
data_size.append(["7 公司名 id 均重复", ETL_business_info.shape])

#% 公司状态
def get_correct_state(x):
    if re.search(r'\[正常|在营|存续|开业|在业]*', x):
        return '在营'
    elif re.search(r'\吊销，未注销*', x):
        return '吊销，未注销'
    elif re.search(r'\吊销*', x):
        return '吊销'
    elif re.search(r'\注销*', x):
        return '注销'
    elif re.search(r'\迁出*', x):
        return '迁出'
    elif re.search(r'\正常*', x):
        return '在营'    
    else :
        return x   # 未公开
    
ETL_business_info['company_operat_state'] = ETL_business_info['company_operat_state'].apply(get_correct_state)

#% 行业分类
economic_category_2011 = sql.read_sql('economic_category_2011', engine).drop('index', axis = 1)
main_category_CN = economic_category_2011[economic_category_2011['category_code'].str.contains(r'[A-Za-z]+')]
#economic_category_2011 = economic_category_2011.set_index('category_name')
economic_category_2011 = economic_category_2011.drop_duplicates('category_name')

ETL_business_info = pd.merge(ETL_business_info,economic_category_2011,
                             how = 'left', left_on = 'company_industry', 
                             right_on = 'category_name').drop(['category_code', 
                                                       'category_name'], axis = 1)
ETL_business_info = pd.merge(ETL_business_info,main_category_CN,
                             how = 'left', on = 'main_category').drop(['company_industry', 
                                                       'main_category'], axis = 1)

#% 公司类型
def get_correct_company_type(x):
    if re.search(r'\个体工商户|个体|其它经济成份联营|民办非企业单位*', x):
        return '内资非法人企业_非公司私营企业_内资非公司企业分支机构'
    elif re.search(r'\农民专业合作经济组织*', x):
        return '其他类型'
    elif re.search(r'\有限责任公司（自然人|一人有限责任公司|有限责任公司（法人独资|其他有限责任公司*', x):
        return '内资公司_有限责任公司'
    elif re.search(r'\其他股份有限公司（非上市）|上市股份有限公司*', x):
        return '内资公司_股份有限公司'    
    elif re.search(r'\股份有限公司分公司（非上市、国有控股）*', x):
        return '内资分公司_股份有限公司'      
    elif re.search(r'\联营（法人）|集体所有制（股份合作）|全民（内联）|机关法人*', x):
        return '内资企业法人'
    elif re.search(r'\外商投资企业分公司|外商投资企业办事处*', x):
        return '外商投资企业_其他'
    elif re.search(r'\有限责任公司（法人独资）（外商投资企业投资）*', x):
        return '外商投资企业_有限责任公司'   
    elif re.search(r'\合资经营（港资）*', x):
        return '港澳台投资企业_其他'    
    else :
        return x   # 未公开
    
company_type_2011 = sql.read_sql('company_type_2011', engine).drop('index', axis = 1)
company_type_2011['company_type_name'] = company_type_2011['company_type_name'].astype(str).apply(lambda x: x.replace('(','（').replace(')','）'))
company_type_2011 = company_type_2011.drop_duplicates('company_type_name')

ETL_business_info['company_type'] = ETL_business_info['company_type'].astype(str).\
                                apply(lambda x: x.replace('(','（').replace(')','）').replace('台港澳','港澳台'))

ETL_business_info = pd.merge(ETL_business_info,company_type_2011,how = 'left', 
              left_on = 'company_type', right_on = 'company_type_name')
company_get = ETL_business_info[ETL_business_info['company_main_type_name'].\
                                isnull()][['company_type','company_type_code',
                                  'company_type_name','company_main_type_name']]    

company_get['company_main_type_name'] = company_get['company_type'].apply(get_correct_company_type)
ETL_business_info = ETL_business_info.combine_first (company_get).drop(['company_type',
                                                    'company_type_code','company_type_name'], axis = 1)

#%% 行政区划
prov_dist_county = sql.read_sql('prov_dist_county_symbol', engine).drop('index', axis = 1)
province = prov_dist_county[['province_symbol','province_name']].drop_duplicates().dropna(how = 'all')
district = prov_dist_county[['district_symbol','district_name']].drop_duplicates().dropna(how = 'all')
county = prov_dist_county[['county_symbol','county_name']].drop_duplicates().dropna(how = 'all')

#%
def get_area(x):
    if x.isdigit():
        if len(x) == 2:
            xx = province[province['province_symbol'].str.contains(r'^%s'%x)]['province_name'].tolist()
            if xx:
                return xx[0]
            else :
                return np.nan
        elif len(x) == 4:
            xx = district[district['district_symbol'].str.contains(r'^%s'%x)]['district_name'].tolist()
            if xx:
                return xx[0]
            else :
                return np.nan   
        elif len(x) == 6:
            xx = county[county['county_symbol'].str.contains(r'^%s'%x)]['county_name'].tolist()
            if xx:
                return xx[0]
            else :
                return np.nan
    else :
        return np.nan
    
ETL_business_info['province_name'] = ETL_business_info['company_area_code'].apply(lambda x:get_area(str(x)[:2]))
ETL_business_info['district_name'] = ETL_business_info['company_area_code'].apply(lambda x:get_area(str(x)[:4]))
ETL_business_info['county_name'] = ETL_business_info['company_area_code'].apply(lambda x:get_area(str(x)[:6]))

#%%
DB_CON_STR = 'mysql+pymysql://root:123456@localhost/sdm_2_mysql?charset=utf8'  
engine = create_engine(DB_CON_STR, echo=False) 

table_name_s = table_name.replace('odm', 'sdm')
sql.to_sql(ETL_business_info, table_name_s, 
           engine, schema='sdm_2_mysql', if_exists='replace') 

#%%  company_base_contact_info_new（企业联系方式）
database_name = 'data_hub_new'
cursor.execute("use "+ database_name) 

table_name = 'company_base_contact_info_new'
cursor.execute("select * from %s"%table_name)
contact_info = as_pandas(cursor)
contact_info = contact_info.drop([0], axis = 0)




#%%





























