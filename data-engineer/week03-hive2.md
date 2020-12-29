# Data Engineering: week 3 Homework Assignment(HIVE-2)

## Question 1

Write queries on banklist table:

```
Find top 5 states with most banks. The result must show the state name and number of banks in descending order.
Found how many banks were closed each year. The result must show the year and the number of banks closed on that year, order by year.
```

## Solution 1

- Find top 5 states with most banks. The result must show the state name and number of banks in descending order.

```sql
SELECT count(*) as number_of_banks, state
FROM banklist
GROUP BY state
ORDER BY number_of_banks DESC
LIMIT 5
```

![1](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/week03_15.png)


- Found how many banks were closed each year. The result must show the year and the number of banks closed on that year, order by year.


Method 1
```sql
CREATE TABLE IF NOT EXISTS pan_db.banklist(
bankname STRING,
city STRING,
state STRING,
cert STRING,
acquiring_institution STRING,
`year` INT
)
stored as parquet;
```
```sql
INSERT INTO TABLE pan_db.banklist
SELECT
bankname,
city,
state,
cert,
acquiring_institution,
year(FROM_UNIXTIME(UNIX_TIMESTAMP(closing_date, 'd-MMM-yy'), 'yyyy-MM-dd'))
from roger_db.banklist;
```
```sql
SELECT count(*) as `number of closed banks`, `year`
FROM pan_db.banklist
GROUP BY `year`
ORDER BY `year` DESC;
```

![2](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/week03_12.png)


Method 2
```sql
SELECT substr(closing_date,-2,2) as yr, count(*) as count
from roger_db.banklist
group by substr(closing_date,-2,2)
ORDER BY yr;
```
![17](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/week03_17.png)


Method 3
```sql
select concat("20",x.yr) as yr,count(*) as number
from 
(
SELECT *, substr(closing_date,-2,2) as yr
from bank_db.banklist
) x
GROUP BY yr
ORDER BY yr desc
```





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
- In your own database, create a partitioned table (partitionedby year) to store data, store in parquet format. Name the table “crime_parquet_16_20”;

```sql
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
stored as parquet;
```
- Import 2016 to 2020 data into the partitioned table from table chicago.crime_parquet

Method 1-Static Partitioning
```sql
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
```sql
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

- Write queries to answer following questions:

- Which type of crime is most occurring for each year?  List top 10 crimes for each year. (use union)
```sql
SELECT primary_type, yr, number 
from  
    (
    SELECT primary_type, yr, count(*) as number
    from pan_db.crime_parquet_16_20
    WHERE yr=2016
    GROUP BY primary_type, yr
    ORDER BY number DESC
    LIMIT 10
    ) t1
UNION
SELECT primary_type, yr, number 
from  
    (
    SELECT primary_type, yr, count(*) as number
    from pan_db.crime_parquet_16_20
    WHERE yr=2017
    GROUP BY primary_type, yr
    ORDER BY number DESC
    LIMIT 10
    ) t2
UNION
SELECT primary_type, yr, number 
from  
    (
    SELECT primary_type, yr, count(*) as number
    from pan_db.crime_parquet_16_20
    WHERE yr=2018
    GROUP BY primary_type, yr
    ORDER BY number DESC
    LIMIT 10
    ) t3
UNION
SELECT primary_type, yr, number 
from  
    (
    SELECT primary_type, yr, count(*) as number
    from pan_db.crime_parquet_16_20
    WHERE yr=2019
    GROUP BY primary_type, yr
    ORDER BY number DESC
    LIMIT 10
    ) t4
UNION
SELECT primary_type, yr, number 
from  
    (
    SELECT primary_type, yr, count(*) as number
    from pan_db.crime_parquet_16_20
    WHERE yr=2020
    GROUP BY primary_type, yr
    ORDER BY number DESC
    LIMIT 10
    ) t5
ORDER BY yr, number DESC;
```
![4](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/week03_9.png)

- Which locations are most likely for a crime to happen?  List top 10 locations.
```sql
SELECT district, count(*) as number
FROM pan_db.crime_parquet_16_20
GROUP BY district
ORDER BY number DESC
LIMIT 10;
```
![5](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/week03_10.png)

- Are there certain high crime rate locations for certain crime types? (use two columns to group by)
```sql
select y.district as district, y.primary_type as primary_type, y.number as number, y.rk as rk
from
    (
    select x.district,
           x.primary_type,
           x.number,
           rank() over (partition by district
                        order by number desc) as rk
    from
        (
        select district, primary_type, count(*) as number
        from pan_db.crime_parquet_16_20
        group by district, primary_type
        ) as x
    ) as y
where rk <= 3
```
![6](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/week03_11.png)

[window function](https://www.postgresqltutorial.com/postgresql-window-function/)

```sql
SELECT district, primary_type, count(*) as number
FROM pan_db.crime_parquet_16_20
GROUP BY district, primary_type
ORDER BY number DESC
LIMIT 10;
```


































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
```sql
DESCRIBE retail_db.categories;
DESCRIBE retail_db.customers;
DESCRIBE retail_db.departments;
DESCRIBE retail_db.orders;
DESCRIBE retail_db.order_items;
DESCRIBE retail_db.products;
```


- List all orders with total order_items = 5.
```sql
SELECT order_item_order_id, sum(order_item_quantity) as `total order_items`
from retail_db.order_items
GROUP BY order_item_order_id
HAVING `total order_items` = 5;
```
![4](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/week03_4.png)


- List customer_id, order_id, order item_count with total order_items = 5

```sql
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
```sql
select x.order_id as order_id, x.number as number, o.order_customer_id as customer_id
from
(
SELECT order_item_order_id as order_id, sum(order_item_quantity) as number
from retail_db.order_items
GROUP BY order_item_order_id
having sum(order_item_quantity)=5
) x
join retail_db.orders o
on x.order_id=o.order_id
```
![5](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/week03_5.png)


- List customer_fname，customer_id, order_id, order item_count with total order_items = 5 (join orders, order_items, customers table)

```sql
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

- List top 10 most popular product categories. (join products, categories, order_items table)
```sql
select c.category_name as category_name, sum(o.order_item_quantity) as category_total
from products as p
join categories as c
on p.product_category_id=c.category_id
join order_items as o
on o.order_item_product_id=p.product_id
GROUP BY category_name
ORDER BY category_total DESC
limit 10;
```
![7](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/week03_14.png)


- List top 10 revenue generating products. (join products, orders, order_items table)
```sql
SELECT product_name, sum(order_item_subtotal) as revenue
FROM orders
JOIN order_items ON orders.order_id = order_items.order_item_order_id
JOIN products ON order_items.order_item_product_id = products.product_id
GROUP BY product_name
ORDER BY revenue DESC
LIMIT 10;
```
![8](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/week03_13.png)

```sql
select p.product_id, p.product_name, r.revenue
from products p 
inner join
(
select oi.order_item_product_id, sum(oi.order_item_subtotal) as revenue
from order_items oi 
inner join orders o
on oi.order_item_order_id = o.order_id
where o.order_status <> 'CANCELED' and o.order_status <> 'SUSPECTED_FRAUD'
group by order_item_product_id
) r
on p.product_id = r.order_item_product_id
order by r.revenue desc
limit 10;
```
```sql
select p.product_id, p.product_name, sum(oi.order_item_subtotal) as revenue
from order_items as oi
join orders as o
on oi.order_item_order_id=o.order_id
join products as p
on p.product_id=oi.order_item_product_id
where o.order_status <> 'CANCELED' and o.order_status <> 'SUSPECTED_FRAUD'
group by p.product_id, p.product_name
order by revenue desc
limit 10
```
![9](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/r1.png)
