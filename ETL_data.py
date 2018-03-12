# -*- coding: utf-8 -*-
"""
Created on Fri Mar  2 10:56:55 2018

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

#% 结果文件夹结构
database_list = ['standard_lib','etl_data','topic_model']
for folder_n in database_list:
    if not os.path.exists(result_folder + '\\' + folder_n):
        os.mkdir(result_folder + '\\' + folder_n)

#%% 获取原始数据：data_hub_new
# 每张表中挑选的字段
file_path = data_folder + '\\4-1 数据仓库（53张表+标准表）.xlsx'
table_col_info = pd.read_excel(file_path, '数据处理', header = 2)
table_col_info['index'] = np.arange(table_col_info.shape[0])
table_col_info.set_index('index', inplace = True)
table_col_info_fillna = table_col_info[['一级分类', '二级分类','数据表名']]
table_col_info_fillna.fillna(method = 'ffill', inplace = True)
table_col_info.update(table_col_info_fillna)
table_col_info['table_name'] = table_col_info['数据表名'].apply(lambda x: x.split('-')[1])

def get_additional_col(col_list):
    additional_col_list = ['chanle_id', 'gather_time', 'company_name']
    for col in additional_col_list:
        if col not in col_list:
            col_list.append(col)
    return col_list
#%%
#% 建立连接
# MySQL
DB_CON_STR = 'mysql+pymysql://root:123456@localhost/my_data?charset=utf8'  
engine = create_engine(DB_CON_STR, echo=False) 

# Hive
conn = connect(host="192.168.20.102", port=10000,  # database="system", 
               auth_mechanism="PLAIN",
               user = 'admin', password = 'admin')
cursor = conn.cursor()

#%% 基本信息：company_base_business_merge_new
# 从hive上取数据
database_name = 'data_hub_new'
cursor.execute("use "+ database_name) 

table_name = 'company_base_business_merge_new'
cursor.execute("select * from %s"%table_name)
busines_info = as_pandas(cursor)
busines_info = busines_info.drop([0], axis = 0)

# 从本地读取CVS文件:company_base_business_merge_new
#folder_name = 'good_table'
#table_name = 'company_base_business_merge_new'
#file_name = table_name + '.csv'
#file_path = os.path.join(os.path.split(data_folder)[0] + '\\' + folder_name,file_name)
#encode = specific_func.get_txt_encode(file_path)
#busines_info = pd.read_csv(file_path, encoding = encode)

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
             'company_credit_code', 'company_regis_code', 
             'company_registration_time','company_address',
             'company_business_scope','company_industry','company_operat_state',
             'company_type','company_registrate_authory']
for col in punc_list:
    busines_info[col] = busines_info[col].apply(handle_punc)

# 提取有用字段
data_size = [] # 每个阶段的数据量及特征量
data_size.append(['1 RAW Data', busines_info.shape])
col_list = []
col_list = table_col_info[table_col_info['table_name'] == table_name]['三级分类（英文名）'].tolist()
col_list = get_additional_col(col_list)
#if 'chanle_id' not in col_list:col_list.append('chanle_id')
#if 'gather_time' not in col_list:col_list.append('gather_time')

ETL_business_info = busines_info[col_list]
data_size.append(['2 提取有用字段', ETL_business_info.shape])

# ['company_name','chanle_id','company_operat_state'] 均不为空
ETL_business_info = ETL_business_info[ETL_business_info['company_name'].notnull() & 
                                      ETL_business_info['chanle_id'].notnull() & 
                                      ETL_business_info['company_operat_state'].notnull()]
data_size.append(["3 公司名、id、公司状态 均不为空", ETL_business_info.shape])

#% 转换时间
ETL_business_info['gather_time'] = pd.to_datetime(ETL_business_info['gather_time'])
#ETL_business_info['gather_time'] = ETL_business_info['gather_time'].apply(lambda x:x.strftime('%Y-%m-%d'))

##%%
#state = []
#for index in ETL_business_info['gather_time'].index:
#    x  = ETL_business_info['gather_time'][index]
#    try :
#        state.append(pd.to_datetime(x))
#    except Exception as e :
#        print(e)
#        print(x)
#        print(index)
#%
# 去除 'company_registration_time' 中字段内容为 ’未公开‘ 的数据
ETL_business_info = ETL_business_info[~ETL_business_info['company_registration_time'].\
                                      astype(str).str.contains('未公开')]
data_size.append(["4 去除 成立日期 中字段内容为 ’未公开‘ 的数据", ETL_business_info.shape])

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
data_size.append(["5 增加衍生变量 公司存在时间（天）", ETL_business_info.shape])

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

#% 行政区划
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

#%% 基本联系信息：company_base_contact_info_new
database_name = 'data_hub_new'
cursor.execute("use "+ database_name) 

table_name = 'company_base_contact_info_new'
cursor.execute("select * from %s"%table_name)
contact_info = as_pandas(cursor)
contact_info = contact_info.drop([0], axis = 0)

#%
# 提取有用字段
data_size = [] # 每个阶段的数据量及特征量
data_size.append(['1 RAW Data', contact_info.shape])
col_list = []
col_list = table_col_info[table_col_info['table_name'] == table_name]['三级分类（英文名）'].tolist()
if 'chanle_id' not in col_list:col_list.append('chanle_id')
if 'company_gather_time' not in col_list:col_list.append('company_gather_time')

ETL_contact_info = contact_info[col_list].copy()
data_size.append(['2 提取有用字段', ETL_contact_info.shape])

# 'chanle_id' 不为空
ETL_contact_info = ETL_contact_info[ETL_contact_info['chanle_id'].notnull()]
data_size.append(["3 id 不为空", ETL_contact_info.shape])

#% 'chanle_id' 不重复
error_list = []
for index in ETL_contact_info.index:
    x = ETL_contact_info['company_gather_time'][index]
    try :
        pd.to_datetime(x)
    except :
        error_list.append(index)
        print('  -- ', x, '  -- ', index)
ETL_contact_info = ETL_contact_info.drop(error_list, axis = 0)
ETL_contact_info['company_gather_time'] = pd.to_datetime(ETL_contact_info['company_gather_time'])
ETL_contact_info = ETL_contact_info.sort_values(by = 'company_gather_time', 
                                                  ascending = False, 
                                                  na_position = 'first')
ETL_contact_info = ETL_contact_info.drop_duplicates(['chanle_id'])
data_size.append(["3 id 不重复", ETL_contact_info.shape])

#% 填补空值：‘暂无’
#ETL_contact_info = ETL_contact_info.fillna(np.nan)
ETL_contact_info = ETL_contact_info.replace('', np.nan)
ETL_contact_info = ETL_contact_info.replace('暂无', np.nan)
ETL_contact_info = ETL_contact_info[ETL_contact_info['company_gather_time'].notnull()]

#%
def get_company_kind(x):
    x = str(x)
    if x == 'nan':
        return np.nan
    elif x == '-':
        return np.nan
    elif re.search(r'\私营|民营|私企*', x):
        return '私营'  
    elif re.search(r'\股份制|股份*', x):
        return '股份制公司'
    elif re.search(r'\国企|国有企业*', x):
        return '国企'
    elif re.search(r'\上市公司*', x):
        return '外商投资企业_有限责任公司'    
    elif re.search(r'\集体企业*', x):
        return '集体企业'    
    elif re.search(r'\外商独资|外企|外资企业*', x):
        return '外商投资企业_有限责任公司'    
    elif re.search(r'\事业单位*', x):
        return '外商投资企业_有限责任公司'    
    elif re.search(r'\有限公司*', x):
        return '有限公司'    
    elif re.search(r'\其它*', x):
        return '其它'    
    else:
        return np.nan  

#error_list = []
#for index in ETL_contact_info.index.tolist():
#    x = ETL_contact_info['company_company_size'][index]
#    try :
#        error_list.append([index,x, get_company_kind(x)])
#    except :
#        error_list.append(index)
#        print('  -- ', x, '  -- ', index)

#%
def get_company_size(x):
    x = str(x)
    if x == 'nan':
        return np.nan
    elif x == '-':
        return np.nan    
    elif re.search(r'\d*-\d*人|\d*人以上|少于\d*人', x):
        return re.search(r'\d*-\d*人|\d*人以上|少于\d*人', x).group() 
    else :
        return np.nan
       
ETL_contact_info['company_kind'] = ETL_contact_info['company_company_size'].apply(get_company_kind)
ETL_contact_info['people_number'] = ETL_contact_info['company_company_size'].apply(get_company_size)
ETL_contact_info = ETL_contact_info.drop('company_company_size', axis = 1)

#% 合并工商基本信息和基本联系信息
ETL_base_business_contact_info = pd.merge(ETL_business_info, ETL_contact_info,
                                         on = 'chanle_id', how = 'left')
sql.to_sql(ETL_base_business_contact_info, 'ETL_base_business_contact_info', 
           engine, schema='my_data', if_exists='replace') 

#%% 工商变更数据
# 从hive上取数据
database_name = 'data_hub_new'
cursor.execute("use "+ database_name) 

table_name = 'company_business_change_new'
cursor.execute("select * from %s"%table_name)
busines_change = as_pandas(cursor)
busines_change = busines_change.drop([0], axis = 0)

#fea_filename = os.path.join(result_folder, table_name + '_' + today + '.xlsx')
#single_fea_desc = cal_func.describe(busines_change,fea_filename, data_rate = 0.1)

#%
# 提取有用字段
data_size = [] # 每个阶段的数据量及特征量
data_size.append(['1 RAW Data', busines_change.shape])
col_list = []
col_list = table_col_info[table_col_info['table_name'] == table_name]['三级分类（英文名）'].tolist()
if 'chanle_id' not in col_list:col_list.append('chanle_id')
if 'company_gather_time' not in col_list:col_list.append('company_gather_time')

ETL_busines_change = busines_change[col_list].copy()
data_size.append(['2 提取有用字段', ETL_busines_change.shape])

# 'chanle_id' 不为空
ETL_busines_change = ETL_busines_change[ETL_busines_change['chanle_id'].notnull()]
data_size.append(["3 id 不为空", ETL_busines_change.shape])


#%% 法律诉讼数据
# 从hive上取数据
database_name = 'data_hub_new'
cursor.execute("use "+ database_name) 

#  COMPANY_EXECUTE_PERSONS  COMPANY_EXECUTIVE_PUNISH
table_name = 'company_court_notice'
cursor.execute("select * from %s"%table_name)
court_notice = as_pandas(cursor)
court_notice = court_notice.drop([0], axis = 0)

fea_filename = os.path.join(result_folder, table_name + '_' + today + '.xlsx')
single_fea_desc = cal_func.describe(court_notice,fea_filename, data_rate = 0.1)
#%%
table_name = 'company_court_session'
cursor.execute("select * from %s"%table_name)
court_session = as_pandas(cursor)
court_session = court_session.drop([0], axis = 0)

fea_filename = os.path.join(result_folder, table_name + '_' + today + '.xlsx')
single_fea_desc = cal_func.describe(court_session,fea_filename, data_rate = 0.1)

#%% 财务数据：
# 财务总览-company_finance_overview
# 利润表-company_profit_statement
# 资产负债表-company_balance_sheet
# 现金流量表-company_statement_cash_flow
# 融资信息-company_financing_info

table_name = ['company_finance_overview','company_profit_statement',
              'company_balance_sheet','company_statement_cash_flow',
              'company_financing_info']

# 从hive上取数据
#database_name = 'data_hub_new'
#cursor.execute("use "+ database_name) 

## 提取有用字段
#data_size = [] # 每个阶段的数据量及特征量
#for table_n in table_name:
#    cursor.execute("select * from %s"%table_n)
#    names['%s' %table_n] = as_pandas(cursor)
#    names['%s' %table_n] = names['%s' %table_n].drop([0], axis = 0)
#    data_size.append([table_n, '1 RAW Data', 
#                      names['%s' %table_n].shape])
#    col_list = []
#    col_list = table_col_info[table_col_info['table_name'] == table_n]['三级分类（英文名）'].tolist()
#    col_list = get_additional_col(col_list)
#    col_list.remove('gather_time')
#    names['ETL_%s' %table_n] = names['%s' %table_n][col_list]
#    data_size.append(['ETL_%s' %table_n, '2 提取有用字段', 
#                      names['ETL_%s' %table_n].shape])

#%
# 从本地读取CVS文件，并取有用字段
folder_name = 'good_table'
data_size = [] # 每个阶段的数据量及特征量
ETL_list = []
ETL_finance = pd.DataFrame()
for table_n in table_name:
    file_name = table_n + '.csv'
    file_path = os.path.join(os.path.split(data_folder)[0] + '\\' + folder_name,file_name)
    encode = specific_func.get_txt_encode(file_path)
    names['%s' %table_n] = pd.read_csv(file_path, encoding = encode)
    data_size.append([table_n, '1 RAW Data', 
                      names['%s' %table_n].shape])
    
    col_list = []
    col_list = table_col_info[table_col_info['table_name'] == table_n]['三级分类（英文名）'].tolist()
    if 'chanle_id' not in col_list : col_list.append('chanle_id')
    names['ETL_%s' %table_n] = names['%s' %table_n][col_list]
    data_size.append(['ETL_%s' %table_n, '2 提取有用字段', 
                      names['ETL_%s' %table_n].shape])
    
    names['ETL_%s' %table_n] = names['ETL_%s' %table_n][~names['ETL_%s' %table_n].duplicated()]
    data_size.append(["3 所有字段均重复", names['ETL_%s' %table_n].shape])    
    
    ### 所有 ['company_name','chanle_id'] 均重复。
    names['ETL_%s' %table_n] = names['ETL_%s' %table_n][~names['ETL_%s' %table_n].duplicated(['chanle_id'])] 
    data_size.append(["4 公司名 id 均重复", names['ETL_%s' %table_n].shape])    
    
#    names['ETL_%s' %table_n] = names['%s' %table_n][col_list].set_index('chanle_id')
#    ETL_list.append(names['ETL_%s' %table_n])
#    ETL_finance = ETL_finance.join()

#%%
result = ETL_finance.join(ETL_list, how = 'outer')   
    
    
    

#%% ETL后数据： etl_data
# 在hive上建
save_filename = result_folder + '\\hadoop语句_etl_data_' + today + '.txt' 
file = open(save_filename,"w")
database_name = 'etl_data'
table_name = ['company_base_business_merge_new']

cursor.execute("create database if not exists {0} ".format(database_name))
cursor.execute("use "+ database_name)
for table_n in table_name:
    # 读入MySQL上的标准表数据，并写入csv文件
    file_name = result_folder +  '\\etl_data\\' + table_n + '.csv'
    names['%s' %table_n] = sql.read_sql(table_n, engine).drop('index', axis = 1)
    names['%s' %table_n].to_csv(file_name, index = False, encoding = 'utf-8', 
                                 sep='^', )
    
    # 在hive上建立标准表 
    cursor.execute('drop table if exists %s;' %table_n)
    field = [x + ' string' for x in names['%s' %table_n].columns.tolist()] 
    sql_code  =  "create external table if not exists {0}{1}".\
            format(table_n,tuple(field)).replace("'","") \
            + '\n' + "ROW FORMAT DELIMITED FIELDS TERMINATED BY '^'" \
            + '\n' + "LOCATION '/tmp/20180302/{0}'".format(table_n)      
    cursor.execute(sql_code)
    
    # 打印在hadoop上操作的语句
#    file.write("load data inpath '/home/hadoop/Public/ETL_data/{0}/{1}.csv' \ # hive 上用
#               into table {1};".format(database_name,table_n) + "\n")
    file.write("hdfs dfs -put -f '/home/hadoop/Public/ETL_data/{0}/{1}.csv' '/tmp/20180302/{1}'".\
               format(database_name,table_n) + "\n")    
# 如果已经put过，需加参数 -f
#hdfs dfs -put -f '/home/hadoop/Public/ETL_data/standard_lib/city_symbol.csv' '/tmp/20180302/city_symbol'
    
file.close()
          
#%%
