# -*- coding: utf-8 -*-
"""
Created on Wed Feb  7 13:41:34 2018

@author: Administrator
"""

#%%
import os
import pandas as pd


data_folder = r'D:\XH\Python_Project\Proj_2\data\DATA'

filename_list = pd.DataFrame(os.listdir(data_folder), columns = ['文件名称'])
filename_list['file_name'] = filename_list['文件名称'].apply(lambda x: os.path.splitext(x)[0])

names = locals()
count = 1
except_list = []

for index in filename_list.index:
    file_name = os.path.join(data_folder, filename_list['文件名称'][index])
    table_name = filename_list['file_name'][index]

    try :
        names['%s' %table_name] = pd.read_csv(file_name) # , nrows =1, encoding = encode
        data_1 = pd.read_csv(file_name)
    except Exception as e:
        print('------------------------------')
        print(e)
        print(index)
        print(file_name)
        print(table_name)   
        except_list.append([index, file_name, table_name, e.args[0]])
        continue         
    
#%%