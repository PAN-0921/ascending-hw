# Data Engineering: week 6 Homework Assignment(Spark DataFrame)

## Employee dataset
In HDFS/data/spark/employee, there are 4 files:
- dept.txt
- dept-with-header.txt
- emp.txt
- emp-with-header.txt
give employee and dept. information.
![99](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/employee_dataset.png)

Answer following questions by:
(1) Spark SQL
(2) Spark DataFrame API


## Question 1
```
list total salary for each dept.
```

## Solution 1
- Create Schemas
```
import org.apache.spark.sql.types._

val dept_schema=new StructType(Array(
new StructField("DEPT_NO",LongType,false),
new StructField("DEPT_NAME",StringType,true),
new StructField("LOC",StringType,true),
))

val emp_schema=new StructType(Array(
new StructField("EMPNO",LongType,false),
new StructField("NAME",StringType,true),
new StructField("JOB",StringType,true),
new StructField("MGR",LongType,false),
new StructField("HIREDATE",DateType,true),
new StructField("SAL",LongType,false),
new StructField("COMM",LongType,false),
new StructField("DEPTNO",LongType,false)
))
```
- Spark read text file into DataFrame
```
val path1="/home/pan/Documents/tutorial/data/dept-with-header.txt"
val dept_with_header_df=spark.read.format("csv").option("header","true").option("mode","FAILFAST").schema(dept_schema).load(path1)
dept_with_header_df.show(truncate=false)

val path2="/home/pan/Documents/tutorial/data/emp-with-header.txt"
val emp_with_header_df=spark.read.option("header","true").option("mode","FAILFAST").schema(emp_schema).csv(path2)
emp_with_header_df.show(truncate=false)
```
- Inner joins
```
val joinExpression=dept_with_header_df.col("DEPT_NO")===emp_with_header_df.col("DEPTNO")
val employee_df=emp_with_header_df.join(dept_with_header_df,joinExpression).drop(emp_with_header_df.col("DEPTNO"))
employee_df.show(false)
```
- Create a Temp View from a dataframe
```
employee_df.createOrReplaceTempView("employee_table")
spark.catalog.listTables.show(false)
spark.sql("select * from employee_table").show
```
![98](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/view.png)


Method 1 - Spark DataFrame API
```
employee_df.groupBy("DEPT_NO","DEPT_NAME").sum("SAL").show(truncate=false)
```


Method 2 - Spark SQL
```
spark.sql("select DEPT_NAME,sum(SAL) as total_salary from employee_table group by DEPT_NAME").show(false)
```
![1](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/Q1.png)







## Question 2
```
list total number of employee and average salary for each dept.
```

## Solution 2
Method 1 - Spark DataFrame API
```
employee_df.count()
import org.apache.spark.sql.functions.{avg}
employee_df.groupBy("DEPT_NAME").avg("SAL").show()
```


Method 2 - Spark SQL
```
spark.sql("select count(*) from employee_table").show(false)
spark.sql("select DEPT_NAME,avg(SAL) as avg_salary from employee_table group by DEPT_NAME").show(false)
```
![2](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/Q2.png)



## Question 3
```
list the first hired employee's name for each dept.
```


## Solution 3
Method 1 - Spark DataFrame API
```
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
val w1=Window.partitionBy("DEPT_NAME").orderBy(col("HIREDATE"))
employee_df.withColumn("row",row_number.over(w1)).where($"row"===1).drop("row").select("NAME","DEPT_NAME").show(false)
```


Method 2 - Spark SQL
```
spark.sql("select s.NAME, s.HIREDATE, s.DEPT_NAME from (select employee_table.NAME, employee_table.HIREDATE, employee_table.DEPT_NAME, ROW_NUMBER() OVER(PARTITION BY employee_table.DEPT_NAME ORDER BY employee_table.HIREDATE) as rk from employee_table) as s where s.rk=1").show(false)
```
![3](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/Q3.png)



## Question 4
```
list total employee salary for each city.
```


## Solution 4
Method 1 - Spark DataFrame API
```
employee_df.groupBy("LOC").sum("SAL").show(false)
```


Method 2 - Spark SQL
```
spark.sql("select sum(SAL), LOC from employee_table group by LOC").show(false)
```
![4](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/Q4.png)





## Question 5
```
list employee's name and salary whose salary is higher than their manager.
```


## Solution 5
Method 1 - Spark DataFrame API
```
val employee2_df=employee_df.withColumnRenamed("SAL","MGR_SAL").withColumnRenamed("NAME","MGR_NAME")
val employee_selfdf=employee_df.as("emp1").join(employee2_df.as("emp2"),col("emp1.MGR") === col("emp2.EMPNO"),"inner").drop(col("emp2.EMPNO"))
employee_selfdf.select("NAME","SAL").where(col("MGR_SAL")<col("SAL")).show
```


Method 2 - Spark SQL
```
employee_selfdf.createOrReplaceTempView("employee_selftable")
spark.catalog.listTables.show(false)
spark.sql("select * from employee_selftable").show
spark.sql("select NAME, SAL from employee_selftable where MGR_SAL<SAL").show(false)
```
![5](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/Q5.png)





## Question 6
```
list employee's name and salary whose salary is higher than average salary of whole company.
```


## Solution 6
Method 1 - Spark DataFrame API
```
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
val AVG_SAL=employee_df.select(avg("SAL")).collect()(0).getDouble(0)//cal the avg value
val employee_avg_df =employee_df.withColumn("AVG_SAL", rand() * 0 +AVG_SAL)//when adding a new column, set the original value as 0, and then add the avg value
employee_avg_df.select("NAME","SAL").where(col("AVG_SAL")<col("SAL")).show
```


Method 2 - Spark SQL
```
spark.sql("select AVG(SAL) from employee_table").show(false)
spark.sql("""
select NAME, SAL
from employee_table
where SAL>2077.0833333333335
""").show(false)
```
![6](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/Q6.png)








## Question 7
```
list employee's name and dept name whose name start with "J".
```


## Solution 7
Method 1 - Spark DataFrame API
```
employee_df.filter(col("NAME").startsWith("J")).select("NAME","DEPT_NAME").show(false)
```


Method 2 - Spark SQL
```
spark.sql("""
select NAME,DEPT_NAME
from employee_table
where NAME LIKE "J%"
""").show(false)
```
![7](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/Q7.png)





## Question 8
```
list 3 employee's name and salary with highest salary.
```


## Solution 8
Method 1 - Spark DataFrame API
```
employee_df.orderBy(col("SAL").desc).select("NAME","SAL").take(3).foreach(println)
```


Method 2 - Spark SQL
```
spark.sql("""
select NAME, SAL
from employee_table
order by SAL DESC
limit 3
""").show(false)
```
![8](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/Q8.png)



## Question 9
```
sort employee by total income (salary+commission), list name and total income.
```


## Solution 9
Method 1 - Spark DataFrame API
```
employee_df.withColumn("totalincome",col("SAL")+col("COMM")).sort(desc("totalincome")).select("NAME","totalincome").show(false)
```


Method 2 - Spark SQL
```
spark.sql("""
select NAME, SAL+COMM as totalincome
from employee_table
order by totalincome DESC
""").show(false)
```
![9](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/Q9.png)
