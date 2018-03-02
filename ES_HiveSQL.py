# -*- coding: utf-8 -*-
"""
Created on Mon Feb  5 09:39:11 2018

@author: Administrator
"""
#%%
import os
import sys
import re
import time
import datetime as dt

today = dt.datetime.now().strftime('%Y%m%d')

import pandas as pd
import numpy as np
import xlrd

#%%
pyfile_folder = r'D:\XH\Python_Project\Proj_2\files'
data_folder = r'D:\XH\Python_Project\Proj_2\data'
result_folder = r'D:\XH\Python_Project\Proj_2\result'

os.chdir(pyfile_folder)
sys.path.append(pyfile_folder)

from Tookits import number_to_chinese  # 数字转中文
from Tookits import num_to_ROMAN_num  # 数字转罗马数字

#%% ES表结构 数据
filename_list = pd.DataFrame(os.listdir(data_folder + '\\数据库表第23版-20180118'), columns = ['文件名称'])
filename_list['file_name'] = filename_list['文件名称'].apply(lambda x: os.path.splitext(x)[0])
filename_list['table_comment'] = filename_list['file_name'].apply(lambda x: x.split('-')[0])
filename_list['table_name'] = filename_list['file_name'].apply(lambda x: x.lower().split('-')[1])

#%% 打开文档
# 42_good_table_name  42_good_table_stat
# 11_bad_table_name  11_bad_table_stat
good_list = pd.read_excel(os.path.join(result_folder,'42_good_table_name.xlsx'))
table_stat_data = pd.read_excel(os.path.join(result_folder,'42_good_table_stat.xlsx'))

save_filename = os.path.join(result_folder, 'ES_HiveSQL.txt')
count = 1
index_list = []
restrain_list = []
ES_data = pd.DataFrame()
file = open(save_filename,"w")
try :
    for index in filename_list.index:
#        index = 0
        table_name = filename_list.loc[index, 'table_name']
        table_comment = filename_list.loc[index, 'table_comment']
        file_name = filename_list.loc[index, '文件名称']
        data_1 = pd.read_excel(os.path.join(data_folder + '\\数据库表第23版-20180118',
                                          file_name), header = None)
        list_filed = [str(x) for x in data_1.iloc[0,:].tolist()]
        
        if '数据库' in ''.join(list_filed):index_list.append(index)
        if index in index_list:
            header = 1
            field_n = '字段名称'
            field_t = '类型'
            field_c = '含义'
            field_e = '空'
            if index in [9,43]: field_e = '为空'
            if index == 30: field_c = '许可部门'
            if index == 40: 
                field_n = '名称'
                field_t = '数据类型'
                field_c = '注释'
                field_e = '非空'
            if index == 43: 
                field_n = '字段名'
                field_t = '字段类型'                
        else :
            header = 0
            field_n = '字段名'
            field_t = '字段类型'
            field_c = '含义'
            field_e = '能否为空'
            if index == 49:field_e = '为空'
        
        data = pd.read_excel(os.path.join(data_folder + '\\数据库表第23版-20180118',
                                          file_name), header = header) 
        data = data.drop_duplicates(subset=field_n, keep='last')
        data = pd.DataFrame(np.array(data),columns = data.columns,index = np.arange(data.shape[0]))
        data[field_e] = data[field_e].fillna('后来补充--')    
        data[field_t] = data[field_t].fillna('未定义')  
        data[field_t] = data[field_t].apply(lambda x:x.lower().strip().split('('))
        # VARCHAR2/varcahr--STRING;  number--NUMBER;  
        data['table_name'] = table_name
        ES_data = pd.concat([ES_data, data], axis = 0)        
        
        if index in index_list:
            data[field_e] = data[field_e].replace('√','否')
            restrain_name = data[data[field_e] == '否'][field_n].tolist()
        else :
            data[field_e] = data[field_e].replace('√','N')
            restrain_name = data[data[field_e] == 'N'][field_n].tolist()
        restrain_name.append('chanle_id')
        restrain_list.append(restrain_name)
            
        file.write('USE data_hub;' + "\n")
        file.write('drop table if exists %s;' %table_name + "\n")
        file.write('CREATE EXTERNAL TABLE `%s` (' %table_name  + "\n")
        for ind in data.index:
            field_name = data.loc[ind, field_n]
            field_name = field_name.replace(' ','_')
            field_type = 'STRING' # data.loc[ind, field_t]            
            field_comment = data.loc[ind, field_c]
            if ind == (data.shape[0]-1):
                punctuation = ')'
            else :
                punctuation = ','
            if field_name == 'number':
                print(field_name)
                field_name = field_name + '_new'
                print(field_name)
            if field_name in restrain_name:
                field_restrain = 'NOT NULL'
            else :
                field_restrain = 'DEFAULT NULL'
            file.write('\t' * count + '%s' %field_name +
                       '\t' * count + '%s' % field_type +
                       '\t' * count + '%s' % field_restrain +
                       '\t' * count + "COMMENT '%s'" % field_comment + punctuation + "\n")
        file.write("COMMENT '%s'" %table_comment + "\n")
        file.write('PARTITIONED BY(dt STRING, country STRING)' + "\n")
        file.write("ROW FORMAT DELIMITED FIELDS TERMINATED BY '$' LOCATION '/tmp/dataclean/%s';" %table_name + "\n")
        file.write("\n" * 2)
        
        print('Index：',index)
        print('----------------------------')
except Exception as e:
    print('++++++++++++++++++++++++++++++++')
    print(e)
    print(index)
    print(file_name)
    print('++++++++++++++++++++++++++++++++')
            
file.close()

#%%
ES_data = pd.DataFrame(np.array(ES_data),
                       columns = ES_data.columns,
                       index = np.arange(ES_data.shape[0]))
ES_data = ES_data.dropna(how = 'all', axis = 1)
ES_data = ES_data.drop('Unnamed: 11', axis = 1)

ES_data['field_name'] = ES_data['字段名称'].combine_first(ES_data['字段名'])
ES_data['field_name'] = ES_data['field_name'].combine_first(ES_data['名称'])

ES_data['field_type'] = ES_data['字段类型'].combine_first(ES_data['数据类型'])
ES_data['field_type'] = ES_data['field_type'].combine_first(ES_data['类型'])

ES_data['field_comment'] = ES_data['含义'].combine_first(ES_data['注释'])
ES_data['field_comment'] = ES_data['field_comment'].combine_first(ES_data['许可部门'])

ES_data['field_empty'] = ES_data['空'].combine_first(ES_data['为空'])
ES_data['field_empty'] = ES_data['field_empty'].combine_first(ES_data['能否为空'])
ES_data['field_empty'] = ES_data['field_empty'].combine_first(ES_data['非空'])

em = ES_data['field_type'].tolist()
emm = [x[0] for x in em]
emmm = np.unique(emm)
emmm = list(set(emm))
#%%
for index in filename_list.index:
    file_name = filename_list.loc[index, '文件名称']
    data = pd.read_excel(os.path.join(data_folder + '\\数据库表第23版-20180118',
                                  file_name), header = None)
    if '数据库中表名' in data.iloc[0,:].tolist()[0]:
        print('Index：',index)
        print(file_name)
        print(data.iloc[0,:].tolist())    
        print('----------------------------')


#%%
document = Document()
document.add_heading('对应42张good_table表中字段含义',0)

duplicate_list = []

table_list = []
for index in good_list.index:  
    table_name = good_list.loc[index, 'file_name']     
    for ind in filename_list.index :#filename_list['table_name'].tolist():
        table_names = filename_list['table_name'][ind]
        table_comment = filename_list['table_comment'][ind]
        if (table_names in table_name) & (table_names not in duplicate_list): # in filename_list['table_name'].tolist():
            duplicate_list.append(table_names)               
            file = table_name + '(' + table_comment +')'
            document.add_heading(str(index + 1) + ' ' + file,1)
            sin_table = table_stat_data[ table_stat_data['table_name'] == table_name][[
                    '字段名','记录量','样本是否缺失']]            
            good_data = ES_data[ES_data['table_name'] == table_names][['field_name',
                               'field_comment','field_type','field_empty']]
            if table_name == 'company_food_license':
#                document.add_paragraph('-- 数据未能统计 --')
                document.add_heading('-- 数据未能统计 --',2)
            else :
#                document.add_paragraph('样本总量为：%s'%str(sin_table.iloc[0,1]))
                document.add_heading('样本量：%s; 特征量：%s'%(str(sin_table.iloc[0,1]),
                                                     str(good_data.shape[0])),2)            
            good_data['备注'] = '-'
            good_data = pd.merge(good_data,sin_table,how = 'left', 
                                 left_on = 'field_name', right_on = '字段名')
            good_data['注意事项'] = '-'
            good_data = good_data.fillna('没有说明')
            good_data = good_data.drop(['字段名','记录量'],axis = 1).applymap(lambda x:str(x))
            #增加表格
            table = document.add_table(rows=good_data.shape[0] + 1,cols=good_data.shape[1])
            for i in range(good_data.shape[0] + 1):
                hdr_cells=table.rows[i].cells
                for j in range(good_data.shape[1]):
                    if i ==0:
                        hdr_cells[j].text = good_data.columns.tolist()[j]
                    else :
                        hdr_cells[j].text = good_data.iloc[i-1,j]
    
    #        good_data.to_excel(os.path.join(r'D:\XH\Python_Project\Proj_2\result\good_table\comment',
    #                                              file+'.xlsx'))  
            print('---------- ',index,table_name)
            
#        else :
#            print('---------- ',index,table_name)
#            table_list.append(table_name)
      
table_list = pd.DataFrame(table_list)
document.save(os.path.join(r'D:\XH\Python_Project\Proj_2\result\good_table\comment',
                           '对应42张good_table表中字段含义.docx'))

#%%     
