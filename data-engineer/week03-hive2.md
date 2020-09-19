# Data Engineering: week 3 Homework Assignment(HIVE-2)

## Question 1

Write queries on banklist table:

```
Find top 5 states with most banks. The result must show the state name and number of banks in descending order.
Found how many banks were closed each year. The result must show the year and the number of banks closed on that year, order by year.
```

## Solution 1

Find top 5 states with most banks. The result must show the state name and number of banks in descending order.

```
SELECT *
FROM
    (
    SELECT count(*) as number_of_banks, state
    FROM banklist
    GROUP BY state
    ) as x
ORDER BY x.number_of_banks DESC
LIMIT 5;
```

![1](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/week03_1.png)


Found how many banks were closed each year. The result must show the year and the number of banks closed on that year, order by year.



```
CREATE TABLE IF NOT EXISTS pan_db.banklist(
bankname STRING,
city STRING,
state STRING,
cert STRING,
acquiring_institution STRING,
`year` INT
)
stored as parquet;

INSERT INTO TABLE pan_db.banklist
SELECT
bankname,
city,
state,
cert,
acquiring_institution,
year(FROM_UNIXTIME(UNIX_TIMESTAMP(closing_date, 'd-MMM-yy'), 'yy-MM-dd'))
from roger_db.banklist;

SELECT *
FROM
    (
    SELECT count(*) as `number of closed banks`, year
    FROM pan_db.banklist
    GROUP BY year
    ) as x
ORDER BY x.year DESC;
```

![2](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/week03_2.png)


## Question 2
`https://data.cityofchicago.org/Public-Safety/Crimes-2020/qzdf-xmn8`

Check hive database named “chicago”, there is one table named: 
crime_parquet: include data from 2001-present
```
1. In your own database, create a partitioned table (partitionedby year) to store data, store in parquet format. Name the table “crime_parquet_16_20”;
2. Import 2016 to 2020 data into the partitioned table from table chicago.crime_parquet
3. Write queries to answer following questions:
Which type of crime is most occurring for each year?  List top 10 crimes for each year.
Which locations are most likely for a crime to happen?  List top 10 locations.
Are there certain high crime rate locations for certain crime types?
```

## Solution 2
In your own database, create a partitioned table (partitionedby year) to store data, store in parquet format. Name the table “crime_parquet_16_20”;

```
create table if not exists pan_db.crime_parquet_16_20 (
   id bigint,
   case_number string,
   `date` string,
   block  string,
   iucr   string,
   primary_type   string,
   description     string,
   loc_desc   string,
   arrest     boolean,
   domestic   boolean,
   beat       string,
   district   string,
   ward       int,
   community_area string,
   fbi_code       string,
   x_coordinate   int,
   y_coordinate   int,
   updated_on     string,
   latitude       float,
   longitude      float,
   loc            string
)
partitioned by (yr int)
stored as parquet；
```
Import 2016 to 2020 data into the partitioned table from table chicago.crime_parquet

Method 1-Static Partitioning
```
set hive.support.quoted.identifiers=none;
insert into table pan_db.crime_parquet_16_20 partition (yr=2016)
select `(yr)?+.+`
from chicago.crime_parquet
WHERE yr=2016
;
insert into table pan_db.crime_parquet_16_20 partition (yr=2017)
select `(yr)?+.+`
from chicago.crime_parquet
WHERE yr=2017
;
insert into table pan_db.crime_parquet_16_20 partition (yr=2018)
select `(yr)?+.+`
from chicago.crime_parquet
WHERE yr=2018
;
insert into table pan_db.crime_parquet_16_20 partition (yr=2019)
select `(yr)?+.+`
from chicago.crime_parquet
WHERE yr=2019
;
insert into table pan_db.crime_parquet_16_20 partition (yr=2020)
select `(yr)?+.+`
from chicago.crime_parquet
WHERE yr=2020
;

show partitions pan_db.crime_parquet_16_20;
```
![3](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/week03_3.png)

Method 2-Dynamic Partitioning
```
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

insert into table pan_db.crime_parquet_16_20 partition (yr)
select 
id, 
case_number, 
`date`, block,iucr, 
primary_type, 
description,
loc_desc, 
arrest, 
domestic,
beat,
district,
ward,
community_area,
fbi_code,
x_coordinate,
y_coordinate,
updated_on,
latitude,
longitude,
loc,
yr
from chicago.crime_parquet
where yr>=2016 and yr<=2020;
```

Write queries to answer following questions:

Which type of crime is most occurring for each year?  List top 10 crimes for each year. (use union)

Which locations are most likely for a crime to happen?  List top 10 locations.

Are there certain high crime rate locations for certain crime types? (use two columns  to group by )


































## Question 3
In retail_db, there are 6 tables.  Get yourself familiar with their schemas.  
categories  
customers  
departments  
orders  
order_items  
products

```
Write queries to answer following questions:
1. List all orders with total order_items = 5.
2. List customer_fname，customer_id, order_id, order item_count with total order_items = 5
3. List customer_fname，customer_id, order_id, order item_count with total order_items = 5 (join orders, order_items, customers table)
4. List top 10 most popular product categories. (join products, categories, order_items table)
5. List top 10 revenue generating products. (join products, orders, order_items table)
```


## Solution 3
```
DESCRIBE retail_db.categories;
DESCRIBE retail_db.customers;
DESCRIBE retail_db.departments;
DESCRIBE retail_db.orders;
DESCRIBE retail_db.order_items;
DESCRIBE retail_db.products;
```


List all orders with total order_items = 5.
```
SELECT order_item_order_id, sum(order_item_quantity) as `total order_items`
from retail_db.order_items
GROUP BY order_item_order_id
HAVING `total order_items` = 5;
```
![4](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/week03_4.png)


List customer_id, order_id, order item_count with total order_items = 5

```
SELECT customer_id, order_id, x.`total order_items`
from orders
join
    (
    SELECT order_item_order_id, sum(order_item_quantity) as `total order_items`
    from retail_db.order_items
    GROUP BY order_item_order_id
    HAVING `total order_items` = 5
    ) as x
on orders.order_id=x.order_item_order_id
join customers
on customers.customer_id=orders.order_customer_id
;
```
![5](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/week03_5.png)


List customer_fname，customer_id, order_id, order item_count with total order_items = 5 (join orders, order_items, customers table)

```
SELECT customer_fname, customer_id, order_id, x.`total order_items`
from orders
join
    (
    SELECT order_item_order_id, sum(order_item_quantity) as `total order_items`
    from retail_db.order_items
    GROUP BY order_item_order_id
    HAVING `total order_items` = 5
    ) as x
on orders.order_id=x.order_item_order_id
join customers
on customers.customer_id=orders.order_customer_id
;
```
![6](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/week03_6.png)

List top 10 most popular product categories. (join products, categories, order_items table)
```
SELECT new.product_category_id as product_category_id, new.category_name as category_name, sum(new.id_total) as category_total
from
    (
    SELECT p.product_category_id as product_category_id, c.category_name as category_name, o.id_total as id_total
    from products as p
    join categories as c
    on p.product_category_id=c.category_id
    join
        (
        SELECT order_item_product_id, sum(order_item_quantity) as id_total
        from order_items
        GROUP BY order_item_product_id
        ) as o
    on o.order_item_product_id=p.product_id
    ) as new
GROUP BY product_category_id, category_name
ORDER BY category_total DESC
limit 10;
```
![7](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/week03_7.png)


List top 10 revenue generating products. (join products, orders, order_items table)
```
SELECT DISTINCT i.id_subtotal, p.product_name
from 
    (
    select i1.order_item_order_id as order_item_order_id, i1.order_item_product_id as order_item_product_id, i2.id_subtotal as id_subtotal
    from
        (
        select order_item_order_id, order_item_product_id
        from order_items
        ) i1
    left join
        (
        select order_item_product_id, sum(order_item_subtotal) as id_subtotal
        from order_items
        GROUP BY order_item_product_id
        ) i2
    on i1.order_item_product_id = i2.order_item_product_id
    ) i
join
    (
    select order_id
    from orders
    WHERE order_status='COMPLETE'
    )
as o
on i.order_item_order_id = o.order_id
join 
    (
    SELECT product_id, product_name
    FROM products
    )as p
on i.order_item_product_id=p.product_id
ORDER BY i.id_subtotal DESC
limit 10;
```
![8](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/week03_8.png)





