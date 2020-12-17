# Hive Hands On

## Create an external table using the dataset
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
### After checking table schema, we can find that all data types are string. 
### The reason is that OpenCSVSerde is used.



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
