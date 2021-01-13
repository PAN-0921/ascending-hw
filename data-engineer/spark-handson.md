# Spark Structured API Lab

## Data Set 

- retail_db: 6 tables
- categories
- customers
- departments
- order_items
- orders
- products


## Question 1
```
1. List all databases in hive;
2. List all tables in retail_db;
3. List columns of all tables, get yourself familiar with all table shcemas
```
## Solution 1
```
spark.catalog.listDatabases().show
spark.catalog.listTables("retail_db").show
spark.catalog.listColumns("retail_db.orders").show
```


## Question 2
```
1. Find out customers who have placed >= 8 orders
2. Use customers table and orders table
3. Order status must be COMPLETE
4. Save output in JSON format, include 3 fields: customer_id, customer_fname, order_count. Sort output by customer_id
5. Output folder is /user/<your_id>/spark-handson/q2
```



## Solution 2 - DataFrame API
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
```
val df1=orders_df.filter("order_status='COMPLETE'")
df1.show
val df2=df1.groupBy("order_customer_id").agg(count("order_id").as("order_count"))
df2.show
val df3=df2.filter("order_count>=8")
df3.show
val df4=df3.join(customers_df, customers_df("customer_id")===orders_df("order_customer_id"),"inner")
df4.show
val df5=df4.select("customer_id","customer_fname","order_count")
df5.show
val df6=df5.sort("customer_id")
df6.show
```
```
spark.conf.set("spark.sql.shuffle.partitions","1")
val output_folder="/user/pan/spark-handson/q2"
df6.write.format("json").mode("overwrite").save(output_folder)
```

## Solution 2 - Spark SQL
```
val q2="""
SELECT customer_id, customer_fname, count(*) as order_count
FROM retail_db.orders as o 
inner join retail_db.customers as c on o.order_customer_id=c.customer_id
where o.order_status='COMPLETE'
group by customer_id, customer_fname
having count(*) >= 8
order by customer_id
"""
```
```
val df7=spark.sql(q2)
```
```
df7.write.format("json").mode("overwrite").save("/user/pan/spark-handson/q2")
```




## Question 3
```
1. Use products table, find out max product_price in each product_category
2. Order the results by max price descending order
3. Save output in Text format with gzip compression. Each line is in following format:product_category_id###max_price
4. Output folder is /user/<your_id>/spark-handson/q30
5. Save output in a different format with snappy compression: product_category_id|max_price, in folder /user/<your_id>/spark-handson/q31. All output must be stored in one file.
6. Save another copy in /user/<your_id>/spark-handson/q32, with header, no compression
7. Load files from /user/<your_id>/spark-handson/q31 or q32 to a dataframe to verify it is correct, define schema when loading
```


## Solution 3 - DataFrame API
```
import org.apache.spark.sql.functions._
```
```
val path="/user/hive/warehouse/retail_db.db/products"
val products_df=spark.read.format("parquet").load(path)
products_df.printSchema
products_df.show(truncate=false)
```
or
```
val products_df=spark.read.table("retail_db.products")
```


### 1,2,3,4
```
val df1=products_df.groupBy("product_category_id").agg(max("product_price").as("max"))
df1.show
```
```
val df2=df1.sort(desc("max"))
df2.show
```
```
val df3=df2.select(concat(col("product_category_id"),lit("###"),col("max")).as("results"))
df3.show
df3.write.format("csv").mode("overwrite").option("compression","gzip").save("/user/pan/spark-handson/q30")
```




### 5 - Method 1
```
spark.conf.set("spark.sql.shuffle.partitions","1")
```
```
val df4=df2.select(concat(col("product_category_id"),lit("|"),col("max")).as("results"))
df4.show
df4.write.format("text").mode("append").option("compression","snappy").save("/user/pan/spark-handson/q31")
```
### 5 - Method 2
```
val df4=df2.select(concat(col("product_category_id"),lit("|"),col("max")).as("results"))
df4.show
df4.coalesce(1).write.format("csv").mode("overwrite").option("compression","snappy").save("/user/pan/spark-handson/q31")
```



### 6
```
df4.coalesce(1).write.format("csv").mode("overwrite").option("header",true).save("/user/pan/spark-handson/q32")
```



### 7
```
import org.apache.spark.sql.types._
val schema=new StructType(Array(
new StructField("product_category_id",StringType,true),
new StructField("max",FloatType,true)
))
val verifyq31=spark.read.format("csv").option("sep","|").option("compression","snappy").option("header",false).schema(schema).load("/user/pan/spark-handson/q31")
verifyq31.show(false)
val verifyq32=spark.read.format("csv").option("sep","|").option("header",true).schema(schema).load("/user/pan/spark-handson/q32")
verifyq32.show(false)
```












## Question 4
```
1. Use orders table, find out orders in 2013-08 with order_status=COLSED. 
2. Pay attention to column order_date, you may need to apply some date/time transformation function to it and create a new column.
3. Calculate the number of orders for each day.  The result dataframe should have 2 columns: order_date and count. Sort the results by order_date in ascending order.
4. Save output in Parquet format in folder /user/<your_id>/spark-handson/q44.Save output to a Hive table named "q4" in your database.
5. Load files from /user/<your_id>/spark-handson/q4 into dataframeto verify it is correct.
```




## Solution 4 - DataFrame API
### 1 - Method 1
```
import org.apache.spark.sql.functions._
val df1=orders_df.withColumn("date",substring(col("order_date"),1,7))
df1.show(truncate=false)
val df2=df1.where("date='2013-08'")
df2.show(truncate=false)
val df3=df2.where("order_status=='CLOSED'")
df3.show(truncate=false)
```
### 1 - Method 2
```
val df1=orders_df.filter("order_date like '2013-08%'")
val df2=df1.filter("order_status='CLOSED'")
```
### 3
```
val df4=orders_df.withColumn("date",substring(col("order_date"),1,10))
df4.show(truncate=false)
val df5=df4.filter("date like '2013-08%'").groupBy("date").count().orderBy(asc("date"))
df5.show(truncate=31)
```
### 4
```
df5.coalesce(1).write.format("parquet").mode("overwrite").save("/user/pan/spark-handson/q4")
```
### 5
```
val df6=spark.read.parquet("/user/pan/spark-handson/q4")
df6.orderBy(asc("date")).show(31)
```
### save as table
```
df6.coalesce(1).write.format("parquet").mode("overwrite").saveAsTable("pan_db.q4")
```

## Solution 4 - Spark SQL
```
val df7=spark.sql("""
select *, substring(order_date,0,7) as date
from retail_db.orders
where substring(order_date,0,7)='2013-08' and order_status='CLOSED'
""")
```
```
val df8=spark.sql("""
select date as order_date, count
from (
select substring(order_date,0,10) as date, count(*) as count
from retail_db.orders
where substring(order_date,0,10) like "2013-08%"
group by substring(order_date,0,10)
	)
order by date
	""")
df8.show(31)
```








## Question 5
```
1. Use products and order_items table, find top 10 products with highest revenue. 
2. Output has 3 field, product_id, product_nameand total_revenue
3. Save output in one CSV file, include header, separate field with ":", save in folder /user/<your_id>/spark-handson/q5
4. Load files from /user/<your_id>/spark-handson/q5 into dataframeto verify it is correct
```




## Solution 5 - DataFrame API
```
spark.conf.set("spark.sql.shuffle.partitions",2)
```
```
val path4="/user/hive/warehouse/retail_db.db/products"
val products_df=spark.read.format("parquet").load(path4)
products_df.printSchema
products_df.show(truncate=false)
```
```
val path3="/user/hive/warehouse/retail_db.db/order_items"
val order_items_df=spark.read.format("parquet").load(path3)
order_items_df.printSchema
order_items_df.show(truncate=false)
```

### 1,2 - Method 1
```
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
```
```
val join_df=order_items_df.join(products_df, products_df("product_id")===order_items_df("order_item_product_id"),"inner")
join_df.printSchema
```
```
val w1=Window.partitionBy("product_name")
val join_df_sum=join_df.withColumn("total_revenue",sum(col("order_item_subtotal")).over(w1)).orderBy(desc("total_revenue")).select("product_name","total_revenue","product_id").distinct
join_df_sum.show(10,false)
```


### 1,2 - Method 2
```
val df1=order_items_df.groupBy("order_item_product_id").agg(sum("order_item_subtotal").as("revenue"))
val df2=df1.orderBy(desc("revenue"))
val df3=df2.limit(10)
val joinExpr=products_df("product_id")===order_items_df("order_item_product_id")
val df4=df3.join(products_df, joinExpr)
val df5=df4.select("product_id","product_name","revenue").sort(desc("revenue"))
```

### 3
```
df5.coalesce(1).write.format("csv").mode("overwrite").option("header",true).option("sep",":").save("/user/pan/spark-handson/q5")
```

### 4
```
val df6=spark.read.format("csv").option("header",true).option("sep",":").option("inferSchema",true).load("/user/pan/spark-handson/q5")
df6.show(false)
```

## Solution 5 - Spark SQL
```
val df7=spark.sql("""
select p.product_id, p.product_name, sum(o.order_item_subtotal) as revenue
from retail_db.order_items as o
join retail_db.products as p
on o.order_item_product_id=p.product_id
group by p.product_id, p.product_name
order by sum(o.order_item_subtotal) desc
limit 10
""")
df7.show(false)
```
