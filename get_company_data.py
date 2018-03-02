# -*- coding: utf-8 -*-
"""
Created on Tue Jan 30 17:26:52 2018

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

from bs4 import BeautifulSoup
import urllib
#%%
pyfile_folder = r'D:\XH\Python_Project\Proj_2\files'
data_folder = r'D:\XH\Python_Project\Proj_2\data\html_data'
result_folder = r'D:\XH\Python_Project\Proj_2\result'

#%%
local_url = data_folder + '\\山东鲁花集团有限公司_【电话地址_招聘信息_注册信息_信用信息_诉讼信息_财务信息】查询-天眼查.html'

html = open(local_url, encoding='utf-8')
html_content = html.read()

soup = BeautifulSoup(html_content, 'html.parser', from_encoding='utf-8')

# 一级目录
first_list = ['公司背景','公司发展','司法风险','经营风险','经营状况','知识产权']

# 二级目录
second_class_lists = ['company-nav-item-enable canClick position-rel ',
                     'company-nav-item-disable position-rel ',
                     'company-nav-item-disable position-rel ',
                     'company-nav-item-disable position-rel ',
                     'company-nav-item-disable position-rel ',
                     'company-nav-item-enable canClick position-rel ']
for class_l in second_class_lists:
    place_name = soup.find_all('div',attrs={'class':'company-nav-item-enable canClick position-rel '})
    place_n = [p.get_text() for p in place_name]


# 三级目录
place_name = soup.find_all('table',attrs={'class':'table companyInfo-table text-center f14'})
place_n = [p.get_text() for p in place_name]

place_name = soup.find_all('div',attrs={'class':'new-c1 pb5'})
place_n = [p.get_text() for p in place_name]

place_name = soup.find_all('table',attrs={'class':'table companyInfo-table text-center f14'})
place_n = [p.get_text() for p in place_name]
#%%