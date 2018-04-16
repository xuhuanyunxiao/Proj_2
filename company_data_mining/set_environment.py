# -*- coding: utf-8 -*-
"""
Created on Fri Mar 23 17:33:16 2018

@author: Administrator
"""

'''
设置环境，包括包、路径等
'''

import re
import sys
import datetime as dt

today = dt.datetime.now().strftime('%Y%m%d')
names = locals()

import pandas as pd
import numpy as np

import xlrd

from sqlalchemy import create_engine
from pandas.io import sql

from impala.dbapi import connect
from impala.util import as_pandas

dir_name = os.path.dirname(__file__)  # 当前文件所在的目录

#%
pyfile_folder = os.path.normpath(dir_name)
data_folder = os.path.normpath(os.path.join(os.path.split(dir_name)[0], 
                                            'data', 'ETL_data'))
result_folder = os.path.normpath(os.path.join(os.path.split(dir_name)[0], 
                                            'result', 'ETL_result'))

os.chdir(os.path.split(dir_name)[0])
sys.path.append(os.path.split(dir_name)[0])

if ('specific_func' not in dir()) | ('cal_func' not in dir()):
    from Tookits import specific_func  
    from Tookits import cal_func

#    pd.set_option('precision', 3)
#    decimals = 3 # 小数位数

try :
    from docx import Document
    from docx.shared import  Pt
    from docx.oxml.ns import  qn
    from docx.shared import Inches
except :
    pass

