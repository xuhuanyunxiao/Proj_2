# -*- coding: utf-8 -*-
"""
Created on Tue Mar 13 10:29:57 2018

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

#%% MySQL 读入数据
DB_CON_STR = 'mysql+pymysql://root:123456@localhost/etl_data_mysql?charset=utf8'  
engine = create_engine(DB_CON_STR, echo=False) 

#% 合并工商基本信息和基本联系信息
ETL_base_business_contact_info = sql.read_sql('etl_base_business_contact_info', 
                                              engine).drop('index', axis = 1)
#%%









#%%
















