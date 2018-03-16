
## ############  1 系统
## #####  路径
select @@datadir; # 查询数据库数据路径
select @@basedir ; # 查询MySQL安装路径
SHOW VARIABLES WHERE Variable_Name LIKE "%dir" ; # 查看所有 dir 路径
show variables like '%plugins%' ;  # 查看插件路径
SELECT @@global.secure_file_priv;
show variables like '%secure%';

## #####  用户
select system_user(); # 查看系统用户 
select current_user(); #  查询当前用户 
select user(); # 查询用户 
select host,user from mysql.user; # 查看所有用户
# 人性化显示所有用户
SELECT DISTINCT CONCAT('User: ''',user,'''@''',host,''';') AS query FROM mysql.user;
# 查看用户的所有权限
#show grants for 'root'@'%';


## #####  系统信息
SELECT version() ; # 查询数据库版本 
SELECT database(); #  查询当前连接的数据库 
select @@version_compile_os; #  查询当前操作系统 
select now(); # 显示当前时间 
show variables; # 显示系统中变量
# net stop mysql; # 关闭mysql服务
# net start mysql; 启动mysql服务


## #####  
# sql语句是否还有优化的余地
explain select * from demand_ques where user_phone = 13973390555;


## #####  存储引擎
show engines; # mysql现在已提供的存储引擎
show variables like '%storage_engine%'; # 默认的存储引擎
# 用了什么引擎(在显示结果里参数engine后面的就表示该表当前用的存储引擎)
show create table demand_ques; # 查看建表语句
# alter table demand_ques engine=innodb; # 修改表引擎


## ############  2 文件导入、导出
# 导入
-- 法一：CMD 
cd F:\software\MySQL\bin  -- 进入MySQL安装目录bin下
mysql   -uroot  –padmin  databaseName  <d:\dump.sql 
-- 法二：CMD 
LOAD DATA local INFILE '/Users/xxx/Downloads/loaddata.txt' 
	IGNORE INTO TABLE testLoadData   
	FIELDS TERMINATED BY ',' 
	ENCLOSED BY '"' 
	LINES TERMINATED BY '\n' 
	ignore 1 lines (username, age, description); 
-- 法一：mysql 命令端
Use databasename;   
Source d:\dump.sql

# 导出
-- CMD命令：mysqldump
cd F:\software\MySQL\bin
mysqldump  –hlocalhost –uroot –p databaseName >d:\dump.sql  -- （后面没有逗号--；）
-- 导出数据库my_data，存为csv文件：
mysqldump -u root -p my_data > D:\XH\OneDrive\MySQL_proj\sql_database\my_data_csv.csv
-- 导出所有数据库，存为sql文件：
mysqldump -u root -p --all-databases > D:\XH\OneDrive\MySQL_proj\sql_database\dump.sql
--导出数据库my_data中的try_data表，存为csv文件：
mysqldump -u root -p my_data try_data 
> D:\XH\OneDrive\MySQL_proj\sql_database\try_data_csv.csv

-- 不带表头csv:
show databases;
use my_data;
select * 
	from try_data into outfile 'D:\\XH\\OneDrive\\MySQL_proj\\sql_database\\try_data_csv_without_column.csv'
	fields terminated by ','
	enclosed by '"'
	lines terminated by '\r\n';

-- 带表头导出csv:
select * 
	from (select 'sepal_length', 'sepal_width', 'petal_length', 'petal_width'
	union all 
	select sepal_length, sepal_width, petal_length, petal_width 
		from try_data) b 
	into outfile 'D:\\XH\\OneDrive\\MySQL_proj\\sql_database\\try_data_csv_with_column.csv' 
	fields terminated by ',' 
	enclosed by '"' 
	lines terminated by '\r\n'; 

## ############  3 数据库
create database new_db; # 创建数据库
drop database new_db; # 删除数据库
show databases; # 查看一个schema中的database名称
use national_school; # 使用某个database
show tables; # 查看database中表的名称
use demand_ques; # 使用某个表



## ############  4 表
# 建表





# 以下三句类似，列出表中所有字段名，及其简要描述
desc demand_ques; 
show columns from demand_ques;
show fields from demand_ques;
select FOUND_ROWS(); # 加上上一句，才是表中字段数（列数），鸡肋

select count(*) from information_schema.`COLUMNS`  # 表中字段数（列数）
where TABLE_SCHEMA = 'national_school' 
and table_name = 'demand_ques';

SELECT * FROM demand_ques;
select count(*) from demand_ques; # 表中被试数（多少行）


## #####  增
alter table 表名 add 列名 类型  # 添加列


## #####  删除与清空
drop talbe demand_ques; # 删除表
delete from demand_ques; # 表还存在，表内容清空
truncate table demand_ques; # 表还存在，表内容清空
alter table 表名 drop column 列名  # 删除列

## #####  改
rename table 原表名 to 新表名； # 修改表名
alter table 表名 modify column 列名 类型;  -- 类型  # 修改列
alter table 表名 change 原列名 新列名 类型; -- 列名，类型  # 修改列
ALTER TABLE testalter_tbl ALTER i SET DEFAULT 1000;  # 修改默认值：
ALTER TABLE testalter_tbl ALTER i DROP DEFAULT; # 删除默认值：
      

## #####  查询
select 教学方法（可多选） as 'teach_method' 
	from demand_ques; # 选一列，并另命名列名
select distinct 教学方法（可多选）_1, 教学方法（可多选）_2 
	from demand_ques; # 选两列，且去重
select count(教学方法（可多选）) 
	from demand_ques; # 选一列，计算行数
select count(distinct 教学方法（可多选）) 
	from demand_ques; # 选一列，去重，计算行数

# 条件查询 where between in not in 
select user_name, user_phone, 教学方法（可多选） 
	from demand_ques 
	where user_phone = 13973390555; # 具体为某值的一条数据
select count(*) 
	from demand_ques 
	where user_phone = 13973390555;

select user_name, user_phone, 教学方法（可多选） 
	from demand_ques 
	where user_phone > 13973390555; # 大于

select user_name, user_phone, 教学方法（可多选） 
	from demand_ques 
	where user_phone between 13973390555 and 15728978435;  # 范围之间

select user_name, user_phone, 教学方法（可多选） 
	from demand_ques 
	where user_phone not in (13973390555, 15728978435, 13947071886);  # 不在元组中

select user_name, user_phone, 教学方法（可多选） 
	from demand_ques  # 另一个表定义范围
	where user_phone in (SELECT 手机号 
		FROM national_school.students_list 
		where 性别 = '女');

select user_name, user_phone, 教学方法（可多选）, 教学方法（可多选）_1 
	from demand_ques
	where 教学方法（可多选）_1 is null; # 是空值
select user_name, user_phone, 教学方法（可多选）, 教学方法（可多选）_1 
	from demand_ques
	where 教学方法（可多选）_1 is not null; # 不是空值


# 简单计算 count + - * / sum max min avg having
select user_phone, user_phone *2 As 'phone * 2' 
	from demand_ques;

# 通配符 like
select user_name, user_phone, 教学方法（可多选） 
	from demand_ques
	where user_name like '张%'; # 匹配多个字符
select user_name, user_phone, 教学方法（可多选） 
	from demand_ques
	where user_name like '张_'; # 只匹配一个字符
select user_name, user_phone, 教学方法（可多选） 
	from demand_ques
	where user_name like '张__'; # 只匹配两个字符


# 限定行数 limit
select user_name, user_phone, 教学方法（可多选） 
	from demand_ques
	limit 5; # 前五行
select user_name, user_phone, 教学方法（可多选） 
	from demand_ques
	limit 3,5; # 第四行开始的五行
select user_name, user_phone, 教学方法（可多选） 
	from demand_ques
	limit 5 offset 3; # 第四行开始的五行


# 排序 order by
select user_name, user_phone, 教学方法（可多选） 
	from demand_ques
	order by user_phone 
	limit 5;
select user_name, user_phone, 教学方法（可多选） 
	from demand_ques
	order by user_phone 
	asc limit 5; # 升序
select user_name, user_phone, 教学方法（可多选） 
	from demand_ques
	order by user_phone 
	desc limit 5; # 降序


# 分组运算 group by
select 培训周期（单选） 
	from demand_ques
	group by 培训周期（单选）;
select 培训周期（单选）, 最近一次国家级培训（单选） 
	from demand_ques
	group by 培训周期（单选）, 最近一次国家级培训（单选）;
select 培训周期（单选）, 最近一次国家级培训（单选） 
	from demand_ques
	where user_phone > 13973390555
	group by 培训周期（单选）, 最近一次国家级培训（单选）;
select 培训周期（单选）, 最近一次国家级培训（单选）, 
	count(*), max(user_phone), min(user_phone), sum(user_phone) 
	from demand_ques
	# where user_phone > 13973390555
	group by 培训周期（单选）, 最近一次国家级培训（单选）
	having max(user_phone) > 13973390555;


## #####  多表查询




