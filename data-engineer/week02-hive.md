# Data Engineering: week 2 Homework Assignment(HIVE-1)

## Question 1

Distinguish HDFS (Hadoop Distributed File System) and Linux File System. Get familiar with HDFS commands. Practice following commands:

```
List directories / files in HDFS
Copy files from Linux File System to HDFS
Copy files from HDFS to Linux File System
```

## Solution 1

List directories / files in HDFS

```bash
ssh pan@54.86.193.122 #ssh to jumpbox 
ssh pan@ip-172-31-92-98.ec2.internal #ssh to edge node
hdfs dfs -ls #list files in HDFS
```

![week0201](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/week02-01.jpg)


Copy files from Linux File system to HDFS
```bash
ls #list files in Linux
```
![2](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/2.jpg)

```bash
hdfs dfs -put 76203dad-263d-4eb2-b2cd-98e0c35de7bb.parquet /user/pan 
```

![3](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/3.jpg)

Copy files from HDFS to Linux File system
```bash
hdfs dfs -get /user/pan/*b.parquet
```
![4](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/4.jpg)
![5](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/5.jpg)


## Question 2
In your hdfs home folder `/user/<your-id>`, create a folder named banklist, and copy `/data/banklist/banklist.csv` to the banklist folder you just created

## Solution 2
```bash
hdfs dfs -mkdir /user/pan/banklist
```
![6](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/6.jpg)

```bash
hdfs dfs -cp /data/banklist/banklist.csv /user/pan/banklist
```
![7](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/7.jpg)


## Question 3
In your database, create a Hive managed table using the CSV files. The CSV file has a header (first line) which gives the name of the columns. Column ‘CERT’ has type ‘int’, other columns have type ‘string’. After create the table, run some queries to verify the table is created correctly. 
```
desc formatted <your table>; 
select * from <yourtable> limit 5; 
select count(*) from <your table>;
```
Hint: Use `org.apache.hadoop.hive.serde2.OpenCSVSerde`. Study the banklist.csv file (hint: some columns have quoted text). Do some research using google to see which SERDEPROPERTIES you need to specify.
## Solution 3
```bash
hdfs dfs -cp /data/banklist/banklist.csv /user/pan/banklist
```
```
create table if NOT EXISTS pan_db.banklist_managed(
`Bank Name` string,
City string,
ST string,
CERT int,
`Acquiring Institution` string,
`Closing Date` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",", 
   "quoteChar"     = "\”", 
   "escapeChar"    = "\\" 
) 
STORED AS TEXTFILE 
tblproperties('skip.header.line.count'='1');

load data inpath '/user/pan/banklist/*.csv' into table pan_db.banklist_managed;

desc formatted pan_db.banklist_managed;
select * from pan_db.banklist_managed
limit 5;
select count(*) from pan_db.banklist_managed;
```

## Question 4
In your database, create a Hive external table using the CSV files. After create the table, run some queries to verify the table is created correctly. 
```
desc formatted <your table>;
select * from <your table> limit 5;
select count(*) from <your table>;
drop table <your table>; verify the data folder is not deleted by Hive.
```
## Solution 4
```bash
hdfs dfs -cp /data/banklist/banklist.csv /user/pan/banklist
```
```
create external table if NOT EXISTS pan_db.banklist_external(
`Bank Name` string,
City string,
ST string,
CERT int,
`Acquiring Institution` string,
`Closing Date` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",", 
   "quoteChar"     = "\”", 
   "escapeChar"    = "\\"
) 
STORED AS TEXTFILE 
location '/user/pan/banklist'
tblproperties('skip.header.line.count'='1')
;

desc formatted pan_db.banklist_external;
SELECT * from pan_db.banklist_external
limit 5;
select count(*) FROM pan_db.banklist_external;
drop table pan_db.banklist_external;
```
## Question 5
Create a Hive table using AVRO file where you get from the SQOOP homework.

## Solution 5
```
CREATE table if NOT EXISTS pan_db.order_items_avro(
order_item_id int,
order_item_order_id int,
order_item_product_id int,
order_item_quantity int,
order_item_subtotal float,
order_item_product_price float
)
STORED AS avro;

load data inpath '/user/pan/retail_db/avro/*.avro' into table pan_db.order_items_avro;

SELECT * from pan_db.order_items_avro
limit 5;
```

