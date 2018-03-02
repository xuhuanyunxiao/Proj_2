# -*- coding: utf-8 -*-
"""
Created on Fri Feb  2 10:20:38 2018

@author: Administrator
"""


#%%
from sqlalchemy import create_engine
from pandas.io import sql

import pandas as pd

#%%
from sklearn.datasets import load_iris
#加载数据集
iris=load_iris()
iris.keys()
#dict_keys(['target', 'DESCR', 'data', 'target_names', 'feature_names'])
#数据的条数和维数
n_samples,n_features=iris.data.shape
print("Number of sample:",n_samples)  
#Number of sample: 150
print("Number of feature",n_features)
#Number of feature 4

#%%
DB_CON_STR = 'mysql+pymysql://root:123456@localhost/my_data?charset=utf8'  
engine = create_engine(DB_CON_STR, echo=False) 

questionnarie_data = pd.DataFrame(iris.data, columns = iris.feature_names)
sql.to_sql(questionnarie_data, 'try_data', engine, schema='my_data', if_exists='replace') 

#%%
