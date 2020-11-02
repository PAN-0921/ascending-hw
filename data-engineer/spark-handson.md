##Question 2
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


##Question 3
```
val path="/user/hive/warehouse/retail_db.db/products"
val products_df=spark.read.format("parquet").load(path)
products_df.printSchema
products_df.show(truncate=false)
```
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
df3.write.format("text").mode("append").option("compression","gzip").save("/user/pan/spark-handson/q30")
```
```
spark.conf.set("spark.sql.shuffle.partitions","1")
```
```
val df4=df2.select(concat(col("product_category_id"),lit("|"),col("max")).as("results"))
df4.show
df4.write.format("text").mode("append").option("compression","snappy").save("/user/pan/spark-handson/q31")
```
```
df4.write.format("text").mode("append").option("header",true)save("/user/pan/spark-handson/q32")
```







import org.apache.spark.sql.types._

val df5_schema=new StructType(Array(
new StructField("results",StringType,false)
))

val path1="/home/pan/Documents/tutorial/data/dept-with-header.txt"
val dept_with_header_df=spark.read.format("csv").option("header","true").option("mode","FAILFAST").schema(dept_schema).load(path1)
dept_with_header_df.show(truncate=false)

```
val path="/user/pan/spark-handson/q32"
val df5=spark.read.format("text").load(path)
val df5=spark.read.format("csv").option("header","true").option("mode","FAILFAST").schema(dept_schema).load(path1)
products_df.printSchema
products_df.show(truncate=false)
```




##Question 4
```
spark.conf.set("spark.sql.shuffle.partitions",2)
```
```
val orders_df=spark.read.table("retail_db.orders")
orders_df.printSchema
orders_df.show(truncate=false)
```
###1
```
import org.apache.spark.sql.functions._
val df1=orders_df.withColumn("date",substring(col("order_date"),1,7))
df1.show(truncate=false)


val df2=df1.where("date='2013-08'")
df2.show(truncate=false)
val df3=df2.where("order_status=='CLOSED'")
df3.show(truncate=false)
```

```
val df4=orders_df.withColumn("date",substring(col("order_date"),1,10))
df4.show(truncate=false)
val df5=df4.groupBy("date").count().orderBy(asc("date"))
df5.show(truncate=false)
df5.write.format("parquet").mode("append").save("/user/pan/spark-handson/q4")
```
```
val df6=spark.read.parquet("/user/pan/spark-handson/q4")
df6.orderBy(asc("date")).show
```



##Question 5
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

###1
- List top 10 revenue generating products. (join products, order_items table)
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




????limit(10)
val result=join_df_sum.withColumn("row",row_number.over()).where("row"<11).drop("row")




```
join_df_sum.write.format("csv").option("header","true").option("mode","FAILFAST").option("sep",":").save("/user/pan/spark-handson/q5")
```

val verify=spark.read.csv("/user/pan/spark-handson/q5")
verify.show(false)


