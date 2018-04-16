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

import xlrd

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
file_path = data_folder + '\\数据流向 1 ODM.xlsx'

excel = xlrd.open_workbook(file_path)
sheet_names = [sheet.name for sheet in excel.sheets()]

table_col_info_all = pd.DataFrame()
for sheet_name in sheet_names:
    if sheet_name != '统计':
        sheet_data = pd.read_excel(file_path, sheet_name, header = 1)
        table_col_info_all = pd.concat([table_col_info_all, sheet_data])

table_col_info_all = table_col_info_all.dropna(how = 'all', axis = 0)
for col in ['类别名','序号' , '表名']:table_col_info_all[col].fillna(method = 'ffill', inplace = True)
table_col_info_all['table_name'] = table_col_info_all['表名'].apply(lambda x: x.split('（')[0])
table_col_info_all['table_comment'] = table_col_info_all['表名'].apply(lambda x: x.replace('）','').split('（')[1])

col_list = ['类别名','序号', '表名','字段名', '字段解释','table_name', 'table_comment']
table_col_info_select = table_col_info_all[col_list][table_col_info_all['是否保留（1：是；0：否）'] == 1]
table_col_info_select = table_col_info_select.sort_values('序号')

#%%
database_name = 'data_hub_new'
cursor.execute("use "+ database_name) 
        
#%  全量
folder_name = 'odm_1' 
count = 1
num = 0

save_filename = os.path.join(result_folder + '\\' + folder_name, 'ODM_1_all_' + today + '.txt')
file = open(save_filename,"w")
file.write('USE odm_1;' + "\n" *2)

table_name_history = []
table_list = [list(x) for x in table_col_info_select[['序号','table_name']].drop_duplicates().values]
#%
for index, table_name in table_list:
    print(index, ' --- ', table_name)
    field_name_comment = table_col_info_select[table_col_info_select['table_name'] == table_name][['字段名','字段解释']].values
    field_name_comment = sorted([list(x) for x in field_name_comment])
    table_comment = table_col_info_select[table_col_info_select['table_name'] == table_name]['table_comment'].unique().tolist()

    try: 
        cursor.execute("select * from %s limit 5"%table_name)
    except :
        table_name = table_name + '_new'
        cursor.execute("select * from %s limit 5"%table_name)    
        
    odm_table_name = 'odm_' + table_name.replace('_new', '')
    num += 1
    file.write('-- ----  %s %s;' %(str(num), table_comment[0]) + "\n")                
    file.write('drop table if exists %s;' %odm_table_name + "\n")
    file.write('create table `%s` ' %odm_table_name  + "\n") 
    file.write('AS'  + "\n")
    file.write('select '  + "\n")
#    file.write("select %s" %', '.join(col_list)  + "\n")    
    for index, [name, commet] in enumerate(field_name_comment):
        if index == (len(field_name_comment) - 1):
            file.write('\t' * count + '%s ' %name + "\n")
        else :
            file.write('\t' * count + '%s, ' %name + "\n")

    file.write("from data_hub_new.%s ;" %table_name + "\n" * 2)
    
    for name,comment in field_name_comment:
        file.write("alter table %s CHANGE COLUMN %s %s STRING comment '%s';" %(odm_table_name,name,name,comment) + "\n")
    file.write("alter table %s SET TBLPROPERTIES ('comment' = '%s');" %(odm_table_name,table_comment[0]) + "\n" * 3)
     
    table_name_history.append([table_comment[0], table_name, odm_table_name])       
file.close()
    
file_name = os.path.join(result_folder + '\\' + folder_name, '源数据和ODM中表名对照_' + today + '.xlsx')
pd.DataFrame(table_name_history, columns = ['表名解释', 'data_hub_new','odm_1'], 
             index = range(1, len(table_name_history)+1)).to_excel(file_name)

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
import numpy as np
randn = np.random.randn
from pandas import *
import matplotlib.pyplot as plt
import matplotlib.colors as colors

idx = np.arange(1,11)
df = pd.DataFrame(company_get['company_main_type_name'].value_counts())
# DataFrame(randn(10, 5), index=idx, columns=['A', 'B', 'C', 'D', 'E'])
vals = np.around(df.values,2)
normal = colors.Normalize(vals.min()-1, vals.max()+1)

fig = plt.figure(figsize=(15,8))
#ax = fig.add_subplot(111, frameon=True, xticks=[], yticks=[])
#fig.set_alpha = 0.1

the_table=plt.table(cellText=vals, rowLabels=df.index, colLabels=df.columns, 
                    colWidths = [0.03]*vals.shape[1], loc='center', 
                    cellColours=plt.cm.GnBu(normal(vals)))
plt.subplots_adjust(left=0.2, bottom=0.2)
plt.show()

#%%

data = [[ 66386, 174296,  75131, 577908,  32015],
        [ 58230, 381139,  78045,  99308, 160454],
        [ 89135,  80552, 152558, 497981, 603535],
        [ 78415,  81858, 150656, 193263,  69638],
        [139361, 331509, 343164, 781380,  52269]]

columns = ('Freeze', 'Wind', 'Flood', 'Quake', 'Hail')
data = pd.DataFrame(data, columns = columns)
print(data.values)
data.head()

#%%
data = pd.DataFrame(company_get['company_main_type_name'].value_counts())

vals = np.round(data.values,2)
normal = colors.Normalize(vals.min()-1, vals.max()+1)

fig = plt.figure()
ax = fig.add_subplot(111, frameon=True, xticks=[], yticks=[])
ax.spines['top'].set_visible(False) #去掉上边框
ax.spines['bottom'].set_visible(False) #去掉下边框
ax.spines['left'].set_visible(False) #去掉左边框
ax.spines['right'].set_visible(False) #去掉右边框

the_table=plt.table(cellText=vals, cellLoc='center', 
                    cellColours=plt.cm.GnBu(normal(vals)), 
                    rowLabels=data.index,  rowColours=None, rowLoc='right', 
                    colLabels=data.columns,colColours=None, colLoc='center', 
                    colWidths = None, 
                    loc='center', bbox=[0, 0, np.floor(data.shape[1]/5 + 1), 
                                        np.floor(data.shape[0]/5)] )  
                                    # [left, bottom, width, height]\
#the_table.auto_set_font_size(False)
#the_table.set_fontsize(fontsize)
                                    #%%
















#%%

