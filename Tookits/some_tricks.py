# -*- coding: utf-8 -*-
"""
Created on Thu Oct 19 10:49:30 2017

@author: xh
"""

#%% -----------------     pandas  ----------------------
import pandas as pd
# 分割DataFrame中的某列数据
users_data = pd.DataFrame((str(x).split('::') for x in pd.DataFrame(users).iloc[:,0]),
                          columns = 'UserID::Gender::Age::Occupation::Zip-code'.split('::'))

stat = pd.pivot_table(students_list,index=["性别","民族","党派"],columns=["省份"],
                      values=["计数"],aggfunc=np.sum,
                      fill_value=0,margins=True)

province = place[place['symbol'].str.contains('0000')]
province.columns = ['province_symbol','province_name']

# 以下相似
province['for_coun'] = province['province_symbol'].apply(lambda x:x[0:2])
combined_data['city_class'] = combined_data['城市级别'].apply(decide_class1) # decide_class1 自建函数
cleaned_data['年份1'] = [decide_value(value) for value in cleaned_data[['体检年','年份']].values] # decide_value 自建函数

place_matrix = pd.merge(place_matrix,district,left_on = 'for_dist', right_on = 'for_coun')

place_matrix['county_name'][place_matrix['county_name'] =='城区'] = \
place_matrix['district_name'][place_matrix['county_name'] =='城区'] + '_' + \
place_matrix['county_name'][place_matrix['county_name'] =='城区'] 

useful_data['学段'] = useful_data['年级'].map({1:'小学低年级', 
                   2:'小学低年级',3:'小学中年级',4:'小学中年级', 5:'小学高年级',
                   6:'小学高年级',7:'初中',8:'初中',9:'初中', 10:'高中', 11:'高中', 12:'高中'})

clearn_data = clearn_data[clearn_data['年级'].isin(ex_list)]

# 转换数据
ages_data = grade_data[[True if (age[g-1] <= int(item) & int(item) <= age[g-1+4] ) else False 
                   for item in grade_data['年龄']]]
stability_samples = stability_samples[(stability_samples['年份'] != 2010)  | (stability_samples['年级'] != 11)]

# 用元组数据做index
tuples = useful_data.columns.tolist()
tupless = [i for i in tuples if (i[1] == value_name) |(i[0] =='年级')|(i[0] =='年龄')|(i[0] =='学段')]
useful_data = useful_data.reindex(columns=pd.MultiIndex.from_tuples(tupless))

# 将某列设为index
address_matrix = pd.DataFrame(address_matrix, columns = ['index','城市级别','城市名']).drop_duplicates(subset=['index']).set_index('index')

#%% -----------------     re  ----------------------
import re
grade = re.findall(r'[[一|二|三|四|五|六]+年级]*|[[初|高]+[一|二|三]+]*',folderName)



#%% -----------------     Excel  ----------------------
with pd.ExcelWriter(data_folder + '\\try_data.xlsx') as writer:
    pd.DataFrame(try_data).to_excel(writer)
    writer.save() 





#%% -----------------     BeautifulSoup、urllib  ----------------------
# 网络：获取六级城市群名称
from bs4 import BeautifulSoup
import urllib

from Tookits.specific_func import find_punctuation

url = 'https://baike.baidu.com/item/%E4%B8%AD%E5%9B%BD%E5%9F%8E%E5%B8%82%E6%96%B0%E5%88%86%E7%BA%A7%E5%90%8D%E5%8D%95/12702007?fr=aladdin#2_6'
response = urllib.request.urlopen(url)
soup = BeautifulSoup(response, 'html.parser', from_encoding='utf-8')

city_name = soup.find_all('div',attrs={'class':'para','label-module':"para"})
city_na = [c.get_text() for c in city_name]
c_names = [city_na[10],city_na[12],city_na[14],city_na[16],city_na[18],city_na[20]]
city = []
for c in c_names:
    punctuation = find_punctuation(pd.Series(c), pattern = u'[\u4e00-\u9fa5]*', del_punc = r'[、]*') # 去除汉字
    if punctuation:
        for p in list(punctuation):c = c.replace(p,'')
    city.append(c.split('、'))

# 本地：获取中国行政区所有省、地区、县的名称
local_url = data_folder + '\\最新县及县以上行政区划代码（截止2016年7月31日）.html'

html = open(local_url, encoding='utf-8')
html_content = html.read()

soup = BeautifulSoup(html_content, 'html.parser', from_encoding='utf-8')

place_name = soup.find_all('p',attrs={'class':'MsoNormal'})
place_n = [p.get_text().split(' ') for p in place_name]
place = pd.DataFrame(place_n, columns = ['symbol','name']).applymap(lambda x: str(x).strip())

#%% -----------------     os  ----------------------
os.listdir(r'c:\windows')
os.getcwd() # 当前工作目录
os.chdir('C:\Users\Python_Folder') # 改变工作目录到dirname
os.curdir # 返回当前工作目录
os.__file__  # D:\envs\py27\lib\os.pyc
os.rename("python26","python21") 
shutil.move("python21","python20") 

os.path.dirname(os.__file__) # 获取路径名
os.path.basename(os.__file__) # 获取文件名
os.path.abspath(os.__file__) # 获得绝对路径
os.path.realpath(os.__file__) #  获取相对路径
os.path.exists('D:\envs\py27\lib') # 路径是否真地存在
os.path.isfile('D:\envs\py27\lib\os.pyc') # 路径是否是一个文件
os.path.isdir('D:\envs\py27\lib') # 路径是否是一个目录
os.path.isabs('D:\envs\py27\lib') # 判断是否是绝对路径

# 创建单个目录 
if not os.path.exists("python27"):
    os.mkdir("python27")   

# 创建多级目录 
if not os.path.exists("python26"):
    os.makedirs(r'python26\test') 

# 只能删除空目录
if os.path.exists("python27"):
    os.rmdir("python27") 

# 删除多个目录    
if os.path.exists("python26"):
    os.removedirs(r'python26\test') 

# 空目录、有内容的目录都可以删 。  
# 递归删除一个目录以及目录内的所有内容
if os.path.exists("python25"):
    shutil.rmtree("python25") 

#%% -----------------     sys  ----------------------






#%% -----------------     MySQL  ----------------------
from sqlalchemy import create_engine
from pandas.io import sql

DB_CON_STR = 'mysql+pymysql://root:123456@localhost/mysql_data?charset=utf8'  
engine = create_engine(DB_CON_STR, echo=False) 

sql.to_sql(questionnarie_data, 'try_data', engine, schema='mysql_data', if_exists='replace') 

# create engine
DB_CON_STR = 'mysql+pymysql://root:123456@localhost/mysql_data?charset=utf8'  
engine = create_engine(DB_CON_STR, echo=False) #True will turn on the logging  
# echo标识用于设置通过python标准日志模块完成的SQLAlchemy日志系统，
# 当开启日志功能，我们将能看到所有的SQL生成代码

# read data form mysql
# data_1 = pd.io.sql.read_sql_table('mysql_mtcars',engine)
table_data = pd.read_sql('mysql_mtcars', engine) # 返回DataFrame

# 表操作    
## 显示已有表名
#pd.read_sql_query('show tables', engine)
#pd.read_sql_query('desc mysql_mtcars', engine)
sql.execute('show tables', engine).fetchall()
sql.execute('desc mysql_mtcars', engine).fetchall()

## 删除表
#pd.read_sql_query('drop table if exists tablename', engine) # 如果表存在则删除
sql.execute('drop table if exists tablename', engine)

## 创建表
sql_cmd = """CREATE TABLE EMPLOYEE (
         FIRST_NAME  CHAR(20) NOT NULL,
         LAST_NAME  CHAR(20),
         AGE INT,  
         SEX CHAR(1),
         INCOME FLOAT )"""
sql.execute(sql_cmd, engine)

# 表内容操作
## 查询/获取
sql_cmd = "SELECT * FROM mysql_mtcars WHERE am = '%d'" % (1)
select_data = pd.read_sql(sql_cmd, engine) # 返回DataFrame

# write data to mysql
#pd.io.sql.to_sql(data,'tablename', engine, schema='mysql_data', if_exists='replace') 
table_data.to_sql('tablename', engine, schema='mysql_data', if_exists='replace') 

#%% -----------------     matplotlib  ----------------------
import matplotlib.pyplot as plt  
def get_plot_data(data,gender,value_name):    
    useful_data = data[data['性别'] == gender].drop(['性别'],axis = 1)        
    tuples = useful_data.columns.tolist()
    tupless = [i for i in tuples if (i[1] == value_name) |(i[0] =='年级')|(i[0] =='年龄')|(i[0] =='学段')]
    useful_data = useful_data.reindex(columns=pd.MultiIndex.from_tuples(tupless))
    
    plot_data = np.array(useful_data)
    plot_data = plot_data[:,1:]

    return plot_data

def get_color_names(xlabel):
#    color_sequence1 = ['#800000', '#FF0000', '#D87093',# 栗色/大红/浅紫红
#              '#FFC0CB', '#CCCC99','#FF8C00',# 粉红/重褐色/深橙色
#              '#BA55D3', '#228B22', '#228B22',# 中紫色/绿色/森林绿
#              '#99CC33', '#008B8B','#000000'] # 间海蓝色/深青色/黑
                       
#2010: 重褐色 #8B4513   2011:秘鲁色 #CD853F  2012：原木色 #DEB887   
#2013:蓟色 #D8BFD8  2014：紫罗兰 #EE82EE  
#2015:石蓝色 #6A5ACD  2016:深洋红色 #8B008B                       
                       
    color_sequence1 = ['#8B4513', '#CD853F', '#DEB887',# 栗色/大红/浅紫红
              '#D8BFD8', '#EE82EE','#6A5ACD',# 粉红/重褐色/深橙色
              '#8B008B', '#228B22', '#228B22',# 中紫色/绿色/森林绿
              '#99CC33', '#008B8B','#000000'] # 间海蓝色/深青色/黑
                       
                       
    color_sequence2 = color_sequence1[:9]   
    legend_names1 = ['一年级','二年级','三年级','四年级','五年级','六年级',
         '初一','初二','初三','高一','高二','高三']
    legend_names2 = ['2010', '2011', '2012', '2013', '2014', '2015', '2016']
    # legend_names3 = ['小学', '中学']
    legend_names3 = ['小学低年级', '小学中年级', '小学高年级', '初中', '高中']

    if xlabel == '年份':
        return color_sequence1,legend_names1
    elif xlabel == '学段':
        return color_sequence1,legend_names3
    else :
        return color_sequence2,legend_names2
    
def get_x_axis(fig, ax,xlabel,data):    
    color_sequence, legend_names = get_color_names(xlabel)    
    if xlabel == '年级':
        ax.set_xlim(1, 13)    
        plt.xticks(range(0, 14, 1), fontsize=14)
        ax.set_xticklabels(['','一年级','二年级','三年级','四年级','五年级','六年级',
                '初一','初二','初三','高一','高二','高三',''])
        plt.xlabel('年级', fontsize=16, ha='center')
        for index,item in enumerate(legend_names):
            plt.plot(np.arange(1,13,1),data[:,index],lw = 1.5,
                     color=color_sequence[index],label = legend_names[index])
    elif xlabel == '年龄':
        ax.set_xlim(1, 15)    
        plt.xticks(range(0, 15, 1), fontsize=14)
        ax.set_xticklabels(['','6','7','8','9','10','11',
                '12','13','14','15','16','17','18','19',''])        
        plt.xlabel('年龄', fontsize=16, ha='center')
        for index,item in enumerate(legend_names):
            plt.plot(np.arange(1,15,1),data[:,index],lw = 1.5,
                     color=color_sequence[index],label = legend_names[index])
    elif xlabel == '年份':
        ax.set_xlim(2010, 2017)    
        plt.xticks(range(2010, 2017, 1), fontsize=14)
        plt.xlabel('年份', fontsize=16, ha='center')    
        for index,item in enumerate(legend_names):
            plt.plot(np.arange(2010,2017,1),data[index,:],lw = 1.5,
                     color=color_sequence[index],label = legend_names[index])
    elif xlabel == '学段':
        ax.set_xlim(2010, 2017)    
        plt.xticks(range(2010, 2017, 1), fontsize=14)
        plt.xlabel('年份', fontsize=16, ha='center')    
        for index,item in enumerate(legend_names):
            plt.plot(np.arange(2010,2017,1),data[index,:],lw = 1.5,
                     color=color_sequence[index],label = legend_names[index])
            
    ax.legend(loc='best', shadow=True,fontsize=14) 
    return fig,ax    

def get_y_axis(fig, ax,fea_name,value_name,plot_data):
    if fea_name == '身高':
        threshold = [190,180]       
        ax.set_ylim(100, threshold[g-1])
        plt.yticks(np.arange(100, threshold[g-1], 10), fontsize=14)    
        ax.yaxis.set_major_formatter(plt.FuncFormatter('{:.0f}'.format))  
        plt.ylabel(fea_name, fontsize=16, ha='center')
    elif fea_name == '体重':
        threshold = [85,75]       
        ax.set_ylim(10, threshold[g-1])
        plt.yticks(np.arange(10, threshold[g-1], 5), fontsize=14)    
        ax.yaxis.set_major_formatter(plt.FuncFormatter('{:.0f}'.format))  
        plt.ylabel(fea_name, fontsize=16, ha='center')
    elif fea_name == 'BMI':
        threshold = [25,25]       
        if value_name == '均值':
            ax.set_ylim(12, threshold[g-1])
            plt.yticks(np.arange(12, threshold[g-1], 1), fontsize=14)    
            ax.yaxis.set_major_formatter(plt.FuncFormatter('{:.0f}'.format))  
            plt.ylabel('BMI 值', fontsize=16, ha='center')
        elif value_name == '肥胖率':
            ax.set_ylim(0, 55)
            plt.yticks(np.arange(0, 55, 5), fontsize=14)    
            ax.yaxis.set_major_formatter(plt.FuncFormatter('{:.0f}%'.format))  
            plt.ylabel('肥胖比例', fontsize=16, ha='center')
            plot_data = plot_data * 100
    elif fea_name == '肺活量': 
        threshold = [4500,3500]            
        if value_name == '均值':
            ax.set_ylim(0, threshold[g-1])
            plt.yticks(np.arange(0, threshold[g-1], 500), fontsize=14)    
            ax.yaxis.set_major_formatter(plt.FuncFormatter('{:.0f}'.format))  
            plt.ylabel('肺活量', fontsize=16, ha='center')
        elif value_name == '不及格率':
            ax.set_ylim(0, 35)
            plt.yticks(np.arange(0, 35, 5), fontsize=14)    
            ax.yaxis.set_major_formatter(plt.FuncFormatter('{:.0f}%'.format))  
            plt.ylabel('不合格比例', fontsize=16, ha='center')
            plot_data = plot_data * 100         
    elif fea_name == '视力':                        
        threshold = [4500,3500]       
        if value_name == '均值':
            ax.set_ylim(0, threshold[g-1])
            plt.yticks(np.arange(0, threshold[g-1], 500), fontsize=14)    
            ax.yaxis.set_major_formatter(plt.FuncFormatter('{:.0f}'.format))  
            plt.ylabel('视力', fontsize=16, ha='center')
        elif value_name == '检出率':
            ax.set_ylim(0, 105)
            plt.yticks(np.arange(0, 110, 10), fontsize=14)    
            ax.yaxis.set_major_formatter(plt.FuncFormatter('{:.0f}%'.format))  
            plt.ylabel('检出率', fontsize=16, ha='center')
            plot_data = plot_data * 100            
            
    return fig, ax,plot_data

def sta_plot(data,xlabel,g,fea_name,value_name,title,filename):
    plot_data = get_plot_data(data,g,value_name)
    
    fig, ax = plt.subplots(1, 1, figsize=(16, 9))    
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    plt.grid(True, 'major', 'y', ls='--', lw=.5, c='k', alpha=.3)
    plt.grid(True, 'major', 'x', ls='--', lw=.5, c='k', alpha=.3)

    fig, ax,plot_data = get_y_axis(fig, ax,fea_name,value_name,plot_data)
    fig, ax = get_x_axis(fig, ax,xlabel,plot_data)
           
    fig.suptitle(title, fontsize=18, ha='center')        
    plt.savefig(filename, bbox_inches='tight')
    plt.show()    

#%% -----------------     matplotlib  ----------------------


