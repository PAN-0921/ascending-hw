# Hive Hands On
## Use beeline command tool to access HiveServer2
```
ssh pan@54.86.193.122
ssh pan@ip-172-31-92-98.ec2.internal
beeline
!connect 'jdbc:hive2://ip-172-31-92-98.ec2.internal:10000'
```

## Create a table stored as avro
```
create table if not exists ying_db.demo1
(
col1 string,
col2 int,
col3 bigint 
)
stored as avro;

insert into table ying_db.demo1
values ("a", 1, 10), ("b", 2, 9);
```

## Create a table stored as textfile
```
create table if not exists ying_db.demo2
(
col1 string,
col2 int,
col3 float
)
row format delimited
fields terminated by ","
lines terminated by "\n"
stored as textfile;

insert into table ying_db.demo2
values ('yingying', 1, 1.0), ('yvonne', 2, 2.0);
```

## Create a managed table
##### copy the parquet file from one directory to another directory within the HDFS 
```
ssh pan@54.86.193.122
ssh pan@ip-172-31-92-98.ec2.internal
hdfs dfs -ls /user/roger/retail_db2/parquet/orders
hdfs dfs -cp /user/roger/retail_db2/parquet/orders/*.parquet /user/pan
hdfs dfs -ls
```
##### Create a managed table and load data using the parquet file
```
create table if not exists ying_db.orders
(
order_id int,
order_date bigint,
order_custome_id int,
order_status string
)
stored as parquet;

load data inpath"/user/pan/*.parquet" into table ying_db.orders;

select * from ying_db.orders limit 5;
```

## Create an external table that stored as parquet
```
hdfs dfs -cp /user/roger/retail_db2/parquet/categories/*.parquet /user/pan/category
hdfs dfs -get /user/roger/retail_db2/parquet/categories/*.parquet
ls categories
parquet-tools schema 0ce28b76-e091-4015-a7e9-412780ee25f0.parquet
```
```
create external table if not exists ying_db.category
(
category_id int,
category_department_id int,
category_name string
)
stored as parquet
location '/user/pan/category'
;

select * from ying_db.category limit 5;
```
##### location 'directory'


## Create a table with an additional column(year), stored as parquet
## Import data from sample_07 and sample_08
## Examine the data
```
create table ying_db.sample_all
(
code string,
description string,
total_emp int,
salary int,
yr int
)
stored as parquet;

desc formatted ying_db.sample_all;
```
```
insert into table ying_db.sample_all
select *,2007 from default.sample_07;

insert into table ying_db.sample_all
select *,2008 from default.sample_08;
```
```
select count(*) from default.sample_07;
select count(*) from default.sample_08;
select count(*) from ying_db.sample_all;
```



## Create a partitioned table, partitioned by year, stored as parquet
## Import data from sample_07 and sample_08
## Examine the data
## add/drop partition (yr=2009)
```
create table ying_db.sample_partitioned
(
code string,
description string,
total_emp int,
salary int
)
partitioned by (yr int)
stored as parquet;

desc formatted ying_db.sample_partitioned;
```
```
insert into table ying_db.sample_partitioned partition(yr=2007)
select * from default.sample_07;

insert into table ying_db.sample_partitioned partition(yr=2008)
select * from default.sample_08;
```
```
select count(*) from ying_db.sample_partitioned;
show partitions ying_db.sample_partitioned;
```
```
alter table ying_db.sample_partitioned 
add partition (yr=2009);
alter table ying_db.sample_partitioned 
drop partition (yr=2009);
```


## Dynamic partition
```
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

truncate table ying_db.sample_partitioned;
show show partitions ying_db.sample_partitioned;

insert into table ying_db.sample_partitioned partition(yr)
select * from sample_all;
```







## Create an external table that stored as textfile by using the dataset
```
CREATE EXTERNAL TABLE IF NOT EXISTS ying_db.crime_19_20(
id bigint,
case_number string,
`date` string,
block string,
iucr string,
primary_type string,
description string,
loc_desc string,
arrest boolean,
domestic boolean,
beat string,
district string,
ward int,
community_area string,
fbi_code string,
x_coordinate int,
y_coordinate int,
yr int,
updated_on string,
latitude float,
longitude float,
loc string
)
ROW FORMAT SERDE
'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES(
"separatorChar"=",",
"quoteChar"="\"",
"escapeChar"="\\"
	)
stored as textfile;
location '/date/chicago_2019_2020/'
tblproperties ("skip.header.line.count"="1");
```
##### After checking table schema, we can find that all data types are string. 
##### The reason is that OpenCSVSerde is used.



## Create a new table, store data in Parquet format
```
CREATE EXTERNAL TABLE IF NOT EXISTS ying_db.crime_parquet(
id bigint,
case_number string,
`date` string,
block string,
iucr string,
primary_type string,
description string,
loc_desc string,
arrest boolean,
domestic boolean,
beat string,
district string,
ward int,
community_area string,
fbi_code string,
x_coordinate int,
y_coordinate int,
yr int,
updated_on string,
latitude float,
longitude float,
loc string
)
stored as parquet;
```

## Import data, pay attention to data type cast
```
insert into table ying_db.crime_parquet
select
cast(id as bigint),
case_number,
unix_timestamp(`date`,'MM/dd/yyyy hh:mm:ss aa'),
block,
iucr,
primary_type,
descrption,
loc_desc,
if(arrest='true',true,false),
if(domestic='true',true,false),
beat,
district,
cast(ward as int),
community_area,
fbi_code,
cast(x_coordinate as int),
cast(y_coordinate as int),
cast(yr as int),
from_unixtime(unix_timestamp(updated_on, 'MM/dd/yyyy hh:mm:ss aa'),'yyyy-MM-dd HH:mm:ss'),
cast(latitude as float),
cast(longitude as float),
loc
from chicago.crime_19_20;
```

## Find out arrest/crime ratio for each year
```
select x.yr as yr, x.arrest_count/y.crime_count as ratio
from
(
select yr,count(*) as arrest_count
from ying_db.crime_parquet
where arrest=true
group by yr
) x
join
(
select yr,count(*) as crime_count
from ying_db.crime_parquet
group by yr
) y
on x.yr=y.yr
```

## Find out arrest/crime ratio for each crime type each year
```
select x.yr as yr, x.primary_type as primary_type, x.arrest_count/y.crime_count as ratio
from
(
select primary_type, yr,count(*) as arrest_count
from ying_db.crime_parquet
where arrest=true
group by yr,primary_type
) x
join
(
select primary_type, yr,count(*) as crime_count
from ying_db.crime_parquet
group by yr,primary_type
) y
on x.yr=y.yr and x.primary_type = y.primary_type
```

## Find out which crime type has lowest arrest/crime ratio for each year
```
select xx.yr as yr, xx.type as type, yy.min_ratio as min_ratio
from
(
select x.yr as yr,x.primary_type as type, x.arrest_count/y.crime_count as ratio
from
	(
	select primary_type, yr,count(*) as arrest_count
	from ying_db.crime_parquet
	where arrest=true
	group by yr,primary_type
	) x
join
	(
	select primary_type, yr,count(*) as crime_count
	from ying_db.crime_parquet
	group by yr,primary_type
	) y
on x.yr=y.yr and x.primary_type = y.primary_type
) xx
join
(
select x.yr as yr, min(x.arrest_count/y.crime_count) as min_ratio
from
	(
	select primary_type, yr,count(*) as arrest_count
	from ying_db.crime_parquet
	where arrest=true
	group by yr,primary_type
	) x
join
	(
	select primary_type, yr,count(*) as crime_count
	from ying_db.crime_parquet
	group by yr,primary_type
	) y
on x.yr=y.yr and x.primary_type = y.primary_type
group by x.yr
) yy
on xx.yr=yy.yr and xx.ratio=yy.min_ratio
order by yr;
```
