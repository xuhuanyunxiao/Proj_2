# -*- coding: utf-8 -*-
"""
Created on Tue Feb 27 10:37:14 2018

@author: Administrator
"""
#%%
import os
import sys
import pandas as pd
from bs4 import BeautifulSoup
import numpy as np

#%
pyfile_folder = r'D:\XH\Python_Project\Proj_2\files'
data_folder = r'D:\XH\Python_Project\Proj_2\data'
result_folder = r'D:\XH\Python_Project\Proj_2\result'

os.chdir(pyfile_folder)
sys.path.append(pyfile_folder)

#%% 本地：获取中国行政区所有省、地区、县的名称
local_url = data_folder + '\\最新县及县以上行政区划代码（截止2016年7月31日）.html'

html = open(local_url, encoding='utf-8')
html_content = html.read()

soup = BeautifulSoup(html_content, 'html.parser', from_encoding='utf-8')

place_name = soup.find_all('p',attrs={'class':'MsoNormal'})
place_n = [p.get_text().split(' ') for p in place_name]
place = pd.DataFrame(place_n, columns = ['symbol','name']).applymap(lambda x: str(x).strip())

province = place[place['symbol'].str.contains('0000')]
province.columns = ['province_symbol','province_name']
province['for_dist'] = province['province_symbol'].apply(lambda x:x[0:2])

district = place[place['symbol'].str.contains(r'[0-9]{2}(?!0000)[0-9]{2}(?=00)')]
district.columns = ['district_symbol','district_name']
district['for_prov'] = district['district_symbol'].apply(lambda x:x[0:2])
district['for_county'] = district['district_symbol'].apply(lambda x:x[0:4])

place_matrix = pd.merge(province,district,left_on = 'for_dist', 
                        right_on = 'for_prov', how = 'outer')

county = place[[True if x not in list(province.index) + list(district.index) else False for x in place.index]]
county.columns = ['county_symbol','county_name']
county['for_dist'] = county['county_symbol'].apply(lambda x:x[0:4])

place_matrix = pd.merge(place_matrix,county,left_on = 'for_county', 
                        right_on = 'for_dist', how = 'outer')

place_matrix['county_name'][place_matrix['county_name'] =='城区'] = \
place_matrix['district_name'][place_matrix['county_name'] =='城区'] + '_' + \
place_matrix['county_name'][place_matrix['county_name'] =='城区'] 

place_matrix['county_name'][place_matrix['county_name'] =='市辖区'] = \
place_matrix['district_name'][place_matrix['county_name'] =='市辖区'] + '_' + \
place_matrix['county_name'][place_matrix['county_name'] =='市辖区'] 

place_matrix['county_name'][place_matrix['county_name'] =='矿区'] = \
place_matrix['district_name'][place_matrix['county_name'] =='矿区'] + '_' + \
place_matrix['county_name'][place_matrix['county_name'] =='矿区'] 

place_matrix['county_name'][place_matrix['county_name'] =='郊区'] = \
place_matrix['district_name'][place_matrix['county_name'] =='郊区'] + '_' + \
place_matrix['county_name'][place_matrix['county_name'] =='郊区'] 

place_matrix['district_name'][place_matrix['district_name'] =='市辖区'] = \
place_matrix['province_name'][place_matrix['district_name'] =='市辖区'] + '_' + \
place_matrix['district_name'][place_matrix['district_name'] =='市辖区'] 

place_matrix['district_name'][place_matrix['district_name'] =='省直辖县级行政区划'] = \
place_matrix['province_name'][place_matrix['district_name'] =='省直辖县级行政区划'] + '_' + \
place_matrix['district_name'][place_matrix['district_name'] =='省直辖县级行政区划'] 

prov_dist_county = place_matrix[['province_symbol', 'district_symbol','county_symbol',
                                 'province_name','district_name','county_name']]
#
prov = prov_dist_county[['province_symbol', 'province_name']].values 
dist = prov_dist_county[['district_symbol', 'district_name']].values 
county = prov_dist_county[['county_symbol', 'county_name']].values 
prov_dist = np.vstack((prov, dist))
tmp = np.vstack((prov_dist,county))

symbol_area = pd.DataFrame(tmp, columns = ['symbol','name']).drop_duplicates().dropna(how = 'all')

#%%
#for sym in place['symbol'].tolist():
#    if sym not in symbol_area['symbol'].tolist():
#        print(sym)


#%%
file_name = '国民经济行业分类与代码.xls'

def assign_code(economic_category_2011):
    position_code = economic_category_2011[economic_category_2011['类别编号'].str.contains(r'[A-Za-z]+')]
    position_code.index.name = '位置'
    position_code = position_code.reset_index()
    for index in position_code.index:
        if index < position_code.index.tolist()[-1]:
            fir = position_code.loc[index, '位置']
            sec = position_code.loc[index + 1, '位置']
            economic_category_2011.loc[fir:sec, '门类'] = position_code.loc[index, '类别编号']
        else :
            fir = position_code.loc[index, '位置']
            economic_category_2011.loc[fir:, '门类'] = position_code.loc[index, '类别编号']
    return economic_category_2011
                
economic_industy_2011 = pd.read_excel(os.path.join(data_folder, file_name),'GB2011',
                                 header = 2,)
economic_industy_2011.columns = economic_industy_2011.columns.tolist()[:4] + ['类别名称','说明']
economic_industy_2011['类别编号'] = economic_industy_2011['门类'].combine_first(economic_industy_2011['大类'])

economic_category_2011 = economic_industy_2011[economic_industy_2011['类别编号'].notnull()][['类别编号','类别名称']]
economic_category_2011 = economic_category_2011.applymap(lambda x: str(x).strip())
economic_category_2011 = assign_code(economic_category_2011)
economic_category_2011.columns = ['category_code','category_name','main_category']

economic_industy_2017 = pd.read_excel(os.path.join(data_folder, file_name),'GB2011-2017新旧对比',
                                 header = 1,)
economic_industy_2017 = economic_industy_2017[['类别编号', '类别名称']].dropna(how = 'any')
economic_industy_2017 = economic_industy_2017.applymap(lambda x: str(x).strip())

#economic_industy_2017[economic_industy_2017['类别编号'].str.contains(r'[A-Za-z]+')]
economic_category_2017 = economic_industy_2017[((economic_industy_2017['类别编号'].str.len() < 2 ) & 
                      (economic_industy_2017.index < 133)) |
                      ((economic_industy_2017['类别编号'].str.len() < 3 ) & 
                      (economic_industy_2017.index > 133))]
economic_category_2017 = assign_code(economic_category_2017)
economic_category_2017.columns = ['category_code','category_name','main_category']

#%% 企业类型
file_name = '企业登记注册类型对照表.xlsx'

company_type = pd.read_excel(os.path.join(data_folder, file_name),header = 2,)
company_type_2011 = company_type.iloc[:,:3]
company_type_2011.columns = ['company_type_code','company_type_name','company_main_type_name']
company_type_2011 = company_type_2011.astype(str).applymap(lambda x: x.strip())
company_type_2011 = company_type_2011[company_type_2011['company_type_code'].str.contains(r'\d{4}')]

#%% 将制作的标准表数据写入Excel、MySQL等存储介质
with pd.ExcelWriter(result_folder + '\\行政区划代码_截止20170731.xlsx') as writer:
    symbol_area.to_excel(writer,'symbol_area')
    prov_dist_county.to_excel(writer,'prov_dist_county')    
    writer.save() 
    
with pd.ExcelWriter(result_folder + '\\经济行业分类.xlsx') as writer:
    company_type_2011.to_excel(writer,'company_type_2011', index = False)
    writer.save()   
    
with pd.ExcelWriter(result_folder + '\\企业登记注册类型.xlsx') as writer:
    economic_category_2017.to_excel(writer,'economic_category_2017', index = False)
    writer.save()     

# 
from sqlalchemy import create_engine
from pandas.io import sql

DB_CON_STR = 'mysql+pymysql://root:123456@localhost/standard_lib_5_mysql?charset=utf8'  
engine = create_engine(DB_CON_STR, echo=False) 

sql.to_sql(symbol_area, 'city_symbol', engine, schema='standard_lib_5_mysql', if_exists='replace') 
sql.to_sql(prov_dist_county, 'prov_dist_county_symbol', engine, schema='standard_lib_5_mysql', if_exists='replace') 
sql.to_sql(economic_category_2017, 'economic_category_2017', engine, schema='standard_lib_5_mysql', if_exists='replace') 
sql.to_sql(economic_category_2011, 'economic_category_2011', engine, schema='standard_lib_5_mysql', if_exists='replace') 
sql.to_sql(company_type_2011, 'company_type_2011', engine, schema='standard_lib_5_mysql', if_exists='replace') 

#%%    
 

