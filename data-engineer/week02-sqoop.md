# Data Engineering: week 2 Homework Assignment(SQOOP)

## Question 1
Get familiar with HUE file browser
[HUE URL](https://hue.ascendingdc.com/hue)

## Question 2
In hdfs, create a folder named retail_db under your home folder `/user/<your-id>`, and 3 sub-folder named parquet, avro and text

## Solution 2
```bash
hdfs dfs -mkdir /user/pan/retail_db
hdfs dfs -mkdir /user/pan/retail_db/parquet
hdfs dfs -mkdir /user/pan/retail_db/avro
hdfs dfs -mkdir /user/pan/retail_db/text
```
![8](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/8.jpg)

## Question 3
Import all tables from MariaDB retail_db to hdfs, in parquet format, save in hdfs folder `/user/<your_id>/retail_db/parquet`, each table is stored in a sub-folder, for example, products table should be imported to `/user/<your-id>/retail_db/parquet/products/`

## Solution 3
```bash
ssh pan@54.86.193.122 #ssh to jumpbox
ssh pan@ip-172-31-92-98.ec2.internal #ssh to edge node
sqoop list-tables --connect jdbc:mysql://database.ascendingdc.com:3306/retail_db --username=student --password=1234abcd #list tables
```
![9](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/9.jpg)

```bash
sqoop import-all-tables \
-m 1 \
--connect jdbc:mysql://database.ascendingdc.com:3306/retail_db \
--username=student \
--password=1234abcd \
--compression-codec=snappy \
--as-parquetfile \
--warehouse-dir /user/pan/retail_db/parquet
```
![10](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/10.jpg)
![11](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/11.jpg)



## Question 4

Import all tables from MariaDB retail_db to hdfs, in text format, use ‘#’ as field delimiter.  Save in hdfs folder `/user/<your_id>/retail_db/text`, each table is stored in a sub-folder, for example, products table should be imported to `/user/<your_id>/retail_db/text/products/`


## Solution 4
```bash
sqoop import-all-tables \
-m 1 \
--connect jdbc:mysql://database.ascendingdc.com:3306/retail_db \
--username=student \
--password=1234abcd \
--fields-terminated-by '#' \
--warehouse-dir /user/pan/retail_db/text
```
![12](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/12.png)
![13](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/13.png)

## Question 5
Import table order_items to hdfs, in avro format, save in hdfs folder `/user/<your_id>/retail_db/avro`
```
First run with one map task, then with two map tasks, compare the console output. List the sqoop behavior difference between one map task and two map tasks.
```

## Solution 5
1. run with one map task
```bash
sqoop import \
-m 1 \
--connect jdbc:mysql://database.ascendingdc.com:3306/retail_db \
--username=student \
--password=1234abcd \
--compression-codec=snappy \
--as-avrodatafile \
--table order_items \
--target-dir /user/pan/retail_db/avro
```
2. run with two map tasks
```bash
sqoop import \
-m 2 \
--connect jdbc:mysql://database.ascendingdc.com:3306/retail_db \
--username=student \
--password=1234abcd \
--compression-codec=snappy \
--as-avrodatafile \
--table order_items \
--target-dir /user/pan/retail_db/avro2
```
3. compare the output. List the sqoop behavior difference between  one map task and two map tasks 

output with one map task: only one data file 

![14](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/14.jpg)

output with two map tasks: two data files
![15](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/15.jpg)


## Question 6
In edge node, in your home folder, create a folder named “order_items_files” in Linux file system.

## Solution 6
```bash
mkdir order_items_files
```
![16](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/16.jpg)

![17](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/17.jpg)

## Question 7
Copy order_items table files generated in step 3, 4, 5 from HDFS to Linux file system, name them as
```
order_items.parquet,
order_items.avro,
order_items.txt
```
 
## Solution 7
1. copy order_items table file generated in step 3 from HDFS to Linux file system, name it as order_items.parquet:
```bash
hdfs dfs -get /user/pan/retail_db/parquet/order_items/8a3761ec-e097-46fa-a553-2bc0c1133797.parquet
mv 8a3761ec-e097-46fa-a553-2bc0c1133797.parquet order_items.parquet
```
2. copy order_items table file generated in step 4 from HDFS to Linux file system, name it as order_items.txt:
```bash
hdfs dfs -get /user/pan/retail_db/text/order_items/part-m-00000
mv part-m-00000 order_items.txt
```

3. copy order_items table file generated in step 5 from HDFS to Linux file system, name it as order_items.avro:
```bash
hdfs dfs -get /user/pan/retail_db/avro/part-m-00000.avro
mv part-m-00000.avro order_items.avro
```

or
```bash
hdfs dfs -get /user/pan/retail_db/avro/part-m-00000.avro order_items.avro
```


![18](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/18.jpg)






## Question 8
Use parquet-tools to show following information from order_items.parquet
```
schema
metadata
rowcount
first 5 records
```
 
## Solution 8
```bash
parquet-tools --help
```

![19](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/19.jpg)
![20](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/20.jpg)
![21](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/21.jpg)
![22](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/22.jpg)

```bash
parquet-tools schema order_items.parquet
```
![23](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/23.jpg)

```bash
parquet-tools meta order_items.parquet
```
![24](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/24.jpg)

```bash
parquet-tools rowcount order_items.parquet
```
![25](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/25.jpg)



```
parquet-tools head -n 5 order_items.parquet
```
![26](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/26.jpg)




## Question 9
Examine text file order_items.txt, and calculate rowcount, compare it with the rowcount in step (8)
## Solution 9
```bash
wc -l order_items.txt
```
![27](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/27.png)

The rowcount in step(8) is the same as  the rowcount in step(9).


## Question 10
Use avro-tools to show following information from order_items.avro
```
schema
metadata
rowcount
convert to json files
```
## Solution 10

```bash
avro-tools --help
```

```bash
avro-tools getschema order_items.avro
```
![28](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/28.jpg)


```bash
avro-tools getmeta order_items.avro
```
![29](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/29.jpg)




```bash
avro-tools tojson order_items.avro | wc -l
```
![30](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/30.png)















