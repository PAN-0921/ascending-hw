# Data Engineering: week 6 Homework Assignment(Spark DataFrame)

## Refer to week 03 hive homework, implement all queries by Spark DataFrame API
```
ssh pan@54.86.193.122 
ssh pan@ip-172-31-92-98.ec2.internal
spark-shell
```
## Question 1

Write queries on banklist table:

```
Find top 5 states with most banks. The result must show the state name and number of banks in descending order.
Found how many banks were closed each year. The result must show the year and the number of banks closed on that year, order by year.
```

## Solution 1
- Create DataFrame from table
```
val banklist_df=spark.read.table("roger_db.banklist").where("bankname != 'Bank Name'")
banklist_df.printSchema
banklist_df.show(truncate=false)
```


- Find top 5 states with most banks. The result must show the state name and number of banks in descending order.
```
val banklist_most_df=banklist_df.select("state").groupBy("state").count().orderBy(desc("count")).limit(5)
banklist_most_df.show
```
![1](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/W6_Q1_1.png)


- Found how many banks were closed each year. The result must show the year and the number of banks closed on that year, order by year.
```
import org.apache.spark.sql.functions._
val banklist_close_df=banklist_df.select(col("Closing Date"),substring(col("Closing Date"),-2,2).alias("yr")).groupBy("yr").count().orderBy(desc("yr"))
banklist_close_df.show
```
![2](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/W6_Q1_2.png)







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
- Create DataFrame from table
```
val crime_parquet_df=spark.read.table("chicago.crime_parquet")
crime_parquet_df.printSchema
crime_parquet_df.show(truncate=false)
```
```
val crime_parquet_16_20_df=crime_parquet_df.where("yr>=2016 and yr<=2020")
crime_parquet_16_20_df.printSchema
crime_parquet_16_20_df.show(truncate=false)
```


- Write queries to answer following questions:

- Which type of crime is most occurring for each year?  List top 10 crimes for each year. (use union)
```
val crime_parquet_16_df=crime_parquet_16_20_df.where("yr=2016").select("primary_type","yr").groupBy("primary_type", "yr").count().orderBy(desc("count")).limit(10)
crime_parquet_16_df.show
val crime_parquet_17_df=crime_parquet_16_20_df.where("yr=2017").select("primary_type","yr").groupBy("primary_type", "yr").count().orderBy(desc("count")).limit(10)
crime_parquet_17_df.show
val crime_parquet_18_df=crime_parquet_16_20_df.where("yr=2018").select("primary_type","yr").groupBy("primary_type", "yr").count().orderBy(desc("count")).limit(10)
crime_parquet_18_df.show
val crime_parquet_19_df=crime_parquet_16_20_df.where("yr=2019").select("primary_type","yr").groupBy("primary_type", "yr").count().orderBy(desc("count")).limit(10)
crime_parquet_19_df.show
val crime_parquet_20_df=crime_parquet_16_20_df.where("yr=2020").select("primary_type","yr").groupBy("primary_type", "yr").count().orderBy(desc("count")).limit(10)
crime_parquet_20_df.show
```
```
val crime_parquet_union_df=crime_parquet_16_df.union(crime_parquet_17_df).union(crime_parquet_18_df).union(crime_parquet_19_df).union(crime_parquet_20_df)
crime_parquet_union_df.show(false)
```
![3](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/W6_Q2_1.png)


or
```
import org.apache.spark.sql.expressions.Window
val df = spark.read.table("chicago.crime_parquet")
val df1 = df.filter("yr>=2016").filter("yr<=2020")
val df2 = df1.groupBy("primary_type","yr").agg(count("id").as("number"))
val w1 = Window.partitionBy("yr").orderBy(desc("number"))
val df3 = df2.withColumn("rk",rank().over(w1))
val df4 = df3.filter("rk<=10").orderBy("yr","rk")
df4.show
```





- Which locations are most likely for a crime to happen?  List top 10 locations.
```
val crime_parquet_loc_df=crime_parquet_16_20_df.select("district").groupBy("district").count().orderBy(desc("count")).limit(10)
crime_parquet_loc_df.show
```
![4](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/W6_Q2_2.png)

or
```
val df = spark.read.table("chicago.crime_parquet")
val df1 = df.filter(expr("yr<=2020") && expr("yr>=2016"))
val df2 = df1.groupBy("district").agg(count("id").as("number"))
val w1 = Window.orderBy(desc("number"))
val df3 = df2.withColumn("rk",rank().over(w1))
val df4 = df3.filter("rk <= 10")
df4.show
```




- Are there certain high crime rate locations for certain crime types? (use two columns to group by)
```
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
```
```
val w1=Window.partitionBy("district").orderBy(desc("count"))
val crime_parquet_loc_type_rank_df=crime_parquet_loc_type_df.withColumn("rk",rank() over(w1)).select("district","primary_type","count","rk").where("rk <=3").orderBy("district","rk")
crime_parquet_loc_type_rank_df.show
```
![5](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/W6_Q2_3.png)





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
2. List customer_id, order_id, order item_count with total order_items = 5
3. List customer_fname，customer_id, order_id, order item_count with total order_items = 5 (join orders, order_items, customers table)
4. List top 10 most popular product categories. (join products, categories, order_items table)
5. List top 10 revenue generating products. (join products, orders, order_items table)
```


## Solution 3


- Use Hive server2 by beeline command tool
```
ssh pan@54.86.193.122
ssh pan@ip-172-31-92-98.ec2.internal
beeline
!connect 'jdbc:hive2://ip-172-31-92-98.ec2.internal:10000'
```


- Check Schemas of tables
```
show databases;
use retail_db;
show tables;
Describe formatted categories;
Describe formatted customers;
Describe formatted departments;
Describe formatted orders;
Describe formatted order_items;
DESCRIBE formatted products;
```


- Create DataFrame from table
```
val customers_df=spark.read.table("retail_db.customers")
customers_df.printSchema
customers_df.show(truncate=false)
```
```
val orders_df=spark.read.table("retail_db.orders")
orders_df.printSchema
orders_df.show(truncate=false)
```


- Spark read parquet file into DataFrame
```
val path1="/user/hive/warehouse/retail_db.db/categories"
val categories_df=spark.read.format("parquet").load(path1)
categories_df.printSchema
categories_df.show(truncate=false)
```
```
val path2="/user/hive/warehouse/retail_db.db/departments"
val departments_df=spark.read.format("parquet").load(path2)
departments_df.printSchema
departments_df.show(truncate=false)
```
```
val path3="/user/hive/warehouse/retail_db.db/order_items"
val order_items_df=spark.read.format("parquet").load(path3)
order_items_df.printSchema
order_items_df.show(truncate=false)
```
```
val path4="/user/hive/warehouse/retail_db.db/products"
val products_df=spark.read.format("parquet").load(path4)
products_df.printSchema
products_df.show(truncate=false)
```


- List all orders with total order_items = 5.
```
val order_items_df_sum=order_items_df.groupBy("order_item_order_id").sum("order_item_quantity").withColumnRenamed("sum(order_item_quantity)","sum_order_item_quantity").filter("sum_order_item_quantity=='5'").orderBy("order_item_order_id")
order_items_df_sum.show
```
or
```
val order_items_df_sum=order_items_df.groupBy("order_item_order_id").agg(sum("order_item_quantity").as("total")).filter("total=5").orderBy("order_item_order_id")
order_items_df_sum.show
```
or
```
order_items_df.groupBy("order_item_order_id").sum("order_item_quantity").filter("sum(order_item_quantity)=5").show
```
![6](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/W6_Q3_1.png)


- List customer_id, order_id, order item_count with total order_items = 5
```
val order_items_df_sum=order_items_df.groupBy("order_item_order_id").sum("order_item_quantity").withColumnRenamed("sum(order_item_quantity)","sum_order_item_quantity").filter("sum_order_item_quantity=='5'").orderBy("order_item_order_id")
order_items_df_sum.show
```
```
orders_df.join(order_items_df_sum, orders_df("order_id")===order_items_df_sum("order_item_order_id"),"inner").join(customers_df, customers_df("customer_id")===orders_df("order_customer_id"),"inner").select("customer_id","order_id","sum_order_item_quantity").show
```
![7](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/W6_Q3_2.png)


- List customer_fname，customer_id, order_id, order item_count with total order_items = 5 (join orders, order_items, customers table)
```
val order_items_df_sum=order_items_df.groupBy("order_item_order_id").sum("order_item_quantity").withColumnRenamed("sum(order_item_quantity)","sum_order_item_quantity").filter("sum_order_item_quantity=='5'").orderBy("order_item_order_id")
order_items_df_sum.show
```
```
orders_df.join(order_items_df_sum, orders_df("order_id")===order_items_df_sum("order_item_order_id"),"inner").join(customers_df, customers_df("customer_id")===orders_df("order_customer_id"),"inner").select("customer_fname","customer_id","order_id","sum_order_item_quantity").show
```
![8](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/W6_Q3_3.png)


- List top 10 most popular product categories. (join products, categories, order_items table)
```
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
```
```
val join_df=products_df.join(categories_df, products_df("product_category_id")===categories_df("category_id"),"inner").join(order_items_df, order_items_df("order_item_product_id")===products_df("product_id"),"inner")
join_df.printSchema
```
```
val w1=Window.partitionBy("category_name")
val join_df_sum=join_df.withColumn("sum",sum(col("order_item_quantity")).over(w1)).orderBy(desc("sum")).select("category_name","sum").distinct
join_df_sum.take(10).foreach(println)
```
![9](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/W6_Q3_4.png)


- List top 10 revenue generating products. (join products, orders, order_items table)
```
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
```
```
val join_df=orders_df.join(order_items_df, orders_df("order_id")===order_items_df("order_item_order_id"),"inner").join(products_df, products_df("product_id")===order_items_df("order_item_product_id"),"inner")
join_df.printSchema
```
```
val w1=Window.partitionBy("product_name")
val join_df_sum=join_df.withColumn("sum",sum(col("order_item_subtotal")).over(w1)).orderBy(desc("sum")).select("product_name","sum").distinct
join_df_sum.take(10).foreach(println)
```
![10](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/W6_Q3_5.png)


