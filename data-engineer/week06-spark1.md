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
List total salary for each dept.
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
- Spark read text file into DataFrame by specify schmema
```
val path1="/home/pan/Documents/tutorial/data/dept-with-header.txt"
val dept_with_header_df=spark.read.format("csv").option("header","true").option("mode","FAILFAST").schema(dept_schema).load(path1)
dept_with_header_df.show(truncate=false)

val path2="/home/pan/Documents/tutorial/data/emp-with-header.txt"
val emp_with_header_df=spark.read.option("header","true").option("mode","FAILFAST").schema(emp_schema).csv(path2)
emp_with_header_df.show(truncate=false)
```
- Spark read text file into DataFrame by infer schema
```
val path1="/data/spark/employee/dept-with-header.txt"
val dept_with_header_df=spark.read.format("csv").option("header","true").option("inferSchema",true).option("sep",",").option("mode","FAILFAST").load(path1)
dept_with_header_df.show(truncate=false)
dept_with_header_df.printSchema()

val path2="/data/spark/employee/emp-with-header.txt"
val emp_with_header_df=spark.read.format("csv").option("header","true").option("inferSchema",true).option("sep",",").option("mode","FAILFAST").load(path2)
emp_with_header_df.show(truncate=false)
emp_with_header_df.printSchema()
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
List total number of employee and average salary for each dept.
```

## Solution 2
Method 1 - Spark DataFrame API
```
employee_df.count()
import org.apache.spark.sql.functions.{avg}
employee_df.groupBy("DEPT_NAME").avg("SAL").show()
```
![2](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/Q2.png)
```
import org.apache.spark.sql.functions._
val Q2_1 = emp_with_header_df.groupBy("DEPTNO").agg(avg("SAL"),count("*").as("number"))
Q2_1.show
```
![199](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/199.png)
```
val join_expr=dept_with_header_df.col("DEPT_NO")===emp_with_header_df.col("DEPTNO")
val join_df=emp_with_header_df.join(dept_with_header_df,join_expr)
join_df.show(false)
val Q2_2=join_df.groupBy("DEPTNO","DEPT_NAME").agg(avg("SAL"),count("*").as("number"))
Q2_2.show
```
![198](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/198.png)

Method 2 - Spark SQL
```
spark.sql("select count(*) from employee_table").show(false)
spark.sql("select DEPT_NAME,avg(SAL) as avg_salary from employee_table group by DEPT_NAME").show(false)
```
```
val Q2_3=spark.sql("""
select DEPTNO, DEPT_NAME, avg(SAL), count(*) as number
from employee_table
group by DEPTNO, DEPT_NAME
""")
Q2_3.show
```



## Question 3
```
List the first hired employee's name for each dept.
```

## Solution 3
Method 1 - Spark DataFrame API
```
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
val w1=Window.partitionBy("DEPT_NAME").orderBy(col("HIREDATE"))
employee_df.withColumn("row",row_number.over(w1)).where($"row"===1).drop("row").select("NAME","DEPT_NAME").show(false)
```
or
```
val Q3_1=emp_with_header_df.groupBy(col("DEPTNO").as("DEPTNO2")).agg(min("HIREDATE").as("min_hiredate"))
Q3_1.show(false)
val join_expr1= (Q3_1.col("DEPTNO2")===emp_with_header_df.col("DEPTNO")) && (Q3_1.col("min_hiredate")===emp_with_header_df.col("HIREDATE"))
val Q3_2=emp_with_header_df.join(Q3_1,join_expr1).select("DEPTNO2","NAME","HIREDATE")
Q3_2.show
```





Method 2 - Spark SQL
```
spark.sql("select s.NAME, s.HIREDATE, s.DEPT_NAME from (select employee_table.NAME, employee_table.HIREDATE, employee_table.DEPT_NAME, ROW_NUMBER() OVER(PARTITION BY employee_table.DEPT_NAME ORDER BY employee_table.HIREDATE) as rk from employee_table) as s where s.rk=1").show(false)
```
or
```
emp_with_header_df.createOrReplaceTempView("emp_table")
val Q3_3=spark.sql("""
select a.DEPTNO, e.NAME, e.hiredate
from 
(
select DEPTNO, min(HIREDATE) as min_hiredate
from emp_table
group by DEPTNO
) as a
join emp_table e
on a.DEPTNO= e.DEPTNO and a.min_hiredate=e.HIREDATE
""")
Q3_3.show(false)
```
![3](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/Q3.png)



## Question 4
```
List total employee salary for each city.
```


## Solution 4
Method 1 - Spark DataFrame API
```
employee_df.groupBy("LOC").sum("SAL").show(false)
```
or
```
val Q4_1=emp_with_header_df.groupBy("DEPTNO").agg(sum("SAL").as("total_salary"))
val join_expr=(Q4_1.col("DEPTNO")===dept_with_header_df.col("DEPT_NO"))
val Q4_2=Q4_1.join(dept_with_header_df,join_expr)
val Q4_3=Q4_2.select("total_salary","LOC")
Q4_3.show(false)
```



Method 2 - Spark SQL
```
spark.sql("select sum(SAL), LOC from employee_table group by LOC").show(false)
```
or
```
emp_with_header_df.createOrReplaceTempView("emp_table")
dept_with_header_df.createOrReplaceTempView("dept_table")
val Q4_4=spark.sql("""
select d.LOC, sum(e.SAL) as total_salary
from emp_table e
join dept_table d
on e.DEPTNO=d.DEPT_NO
group by d.LOC
""")
Q4_4.show(false)
```
![4](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/Q4.png)





## Question 5
```
List employee's name and salary whose salary is higher than their manager.
```


## Solution 5
Method 1 - Spark DataFrame API
```
val employee2_df=employee_df.withColumnRenamed("SAL","MGR_SAL").withColumnRenamed("NAME","MGR_NAME")
val employee_selfdf=employee_df.as("emp1").join(employee2_df.as("emp2"),col("emp1.MGR") === col("emp2.EMPNO"),"inner").drop(col("emp2.EMPNO"))
employee_selfdf.select("NAME","SAL").where(col("MGR_SAL")<col("SAL")).show
```










???
```
val Q5_1=employee_df.select(col("EMPNO"),col("SAL").as("MGR_SAL"))
val join_expr=(Q5_1.col("EMPNO")===employee_df.col("MGR")) && (col("MGR_SAL")<col("SAL"))
val Q5_2=employee_df.join(Q5_1, join_expr)
val Q5_3=Q5_2.select("NAME","SAL")
Q5_3.show(false)
```

val df_q5_1 = employee_df.selectExpr("EMPNO as emp_no", "SAL as salary")
val join_expr_5 = (employee_df.col("MGR") === df_q5_1.col("emp_no")) && (employee_df.col("SAL") > df_q5_1.col("salary"))
val df_q5_2 = employee_df.join(df_q5_1, join_expr_5).select("NAME", "SAL")
df_q5_2.show












Method 2 - Spark SQL
```
employee_selfdf.createOrReplaceTempView("employee_selftable")
spark.catalog.listTables.show(false)
spark.sql("select * from employee_selftable").show
spark.sql("select NAME, SAL from employee_selftable where MGR_SAL<SAL").show(false)
```
or
```
spark.sql("""
select e1.NAME, e1.SAL
from employee_table e1 INNER JOIN employee_table e2 on e1.MGR==e2.EMPNO
where e1.SAL>e2.SAL
""").show(false)
```
or
```
emp_with_header_df.createOrReplaceTempView("emp_table")
val Q5_5=spark.sql("""
select e1.NAME, e1.SAL
from emp_table e1
join emp_table e2
on e1.MGR=e2.EMPNO
where e1.SAL>e2.SAL
""")
Q5_5.show(false)
```
![5](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/Q5.png)





## Question 6
```
List employee's name and salary whose salary is higher than average salary of whole company.
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
or
```
val w1=Window.partitionBy()
val employee_avg_df=employee_df.withColumn("AVG_SAL", avg(col("SAL")).over(w1)).select("NAME","SAL").where(col("AVG_SAL")<col("SAL"))
employee_avg_df.show(false)
```
or
```
val Q6_1=emp_with_header_df.select(avg("SAL"))
val avg_sal=Q6_1.first().getDouble(0)
val Q6_2=emp_with_header_df.select("NAME","SAL").where(col("SAL")>lit(avg_sal))
Q6_2.show
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
or
```
spark.sql("""
select NAME, SAL
from employee_table
where SAL>(select AVG(SAL) from employee_table)
""").show(false)
```
or
```
spark.sql("""
select NAME, SAL
from(
select NAME, SAL, AVG(SAL) OVER () AS AVG_SAL
from employee_table
)
where SAL > AVG_SAL
""").show(false)
```

???
```
emp_with_header_df.createOrReplaceTempView("emp_table")
val Q6=spark.sql("""
select name, sal
from emp_table e1
join (select avg(sal) as avg_sal from emp_table) e2
on e1.sal>e2.avg_sal
""")
Q6.show(false)
```


![6](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/Q6.png)








## Question 7
```
List employee's name and dept name whose name start with "J".
```


## Solution 7
Method 1 - Spark DataFrame API
```
employee_df.filter(col("NAME").startsWith("J")).select("NAME","DEPT_NAME").show(false)
```
or


???
```
val Q7_1=emp_with_header_df.where("NAME LIKE "J%"")
val join_expr = (Q7_1.col("DEPTNO")===dept_with_header_df.col("DEPT_NO"))
val Q7_2=Q7_1.join(dept_with_header_df,join_expr).select("NAME","DEPT_NAME")
Q7_2.show
```



Method 2 - Spark SQL
```
spark.sql("""
select NAME,DEPT_NAME
from employee_table
where NAME LIKE "J%"
""").show(false)
```


???
```
emp_with_header_df.createOrReplaceTempView("emp_table")
dept_with_header_df.createOrReplaceTempView("dept_table")
val Q7=spark.sql("""
select e.NAME, d.DEPT_NAME
from emp_table e
join dept_table d
on e.DEPTNO=d.DEPT_NO
where e.name like "J%"
""")
Q7.show
```



![7](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/Q7.png)





## Question 8
```
List 3 employee's name and salary with highest salary.
```


## Solution 8
Method 1 - Spark DataFrame API
```
employee_df.orderBy(col("SAL").desc).select("NAME","SAL").take(3).foreach(println)
```


????
```
val Q8=emp_with_header_df.orderBy(desc("SAL")).select("NAME","SAL").limit(3)
Q8.show
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
Sort employee by total income (salary+commission), list name and total income.
```


## Solution 9
Method 1 - Spark DataFrame API
```
employee_df.withColumn("totalincome",col("SAL")+col("COMM")).sort(desc("totalincome")).select("NAME","totalincome").show(false)
```

???
```
val Q9=emp_with_header_df.select("NAME",("SAL"+"COMM").as("total_income")).ordeyBy(desc("total_income"))
Q9.show
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
