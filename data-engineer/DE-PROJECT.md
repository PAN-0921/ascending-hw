# Data Engineering Project

## Knowledge required for this project
```
- Kafka
- Spark
- DataFrame transformation
- Spark Structured Streaming
- Impala
- Hdfs
- Kudu
- YARN
```
## Create a Kafka Topic

```
Create a kafka topic named "rsvp_your_id" with 2 partitions and 2 replications.
```

## Solution 

```
ssh pan@54.86.193.122 
ssh pan@ip-172-31-92-98.ec2.internal
kafka-topics --bootstrap-server ip-172-31-94-165.ec2.internal:9092 --create --topic rsvp_pan --replication-factor 2 --partitions 2
kafka-topics --bootstrap-server ip-172-31-94-165.ec2.internal:9092 --list
kafka-topics --bootstrap-server ip-172-31-94-165.ec2.internal:9092 --describe --topic rsvp_pan
```




## Create Kafka Data Source

```
Use meetup RSVP event as the Kafkadata source. 
Ingest meetup_rsvp event to the Kafka topic, use kafka-console-consumer to verify it.
Meetup provides a streaming API for retrieving the RSVP event.
https://stream.meetup.com/2/rsvps
```
```
Hint:
The easiest way is to use kafka-console-producer
Bonus:
Write a Kafka producer and a Kafka consumer using Scala, or Java, or Python to produce/consume events to/from the Kafka topic.
```

## Solution 
use kafka-console-producer
```
curl https://stream.meetup.com/2/rsvps | kafka-console-producer --broker-list ip-172-31-94-165.ec2.internal:9092,ip-172-31-91-232.ec2.internal:9092,ip-172-31-89-11.ec2.internal:9092 --topic rsvp_pan 
```

use kafka-console-consumer to verify it
```
kafka-console-consumer --bootstrap-server ip-172-31-89-11.ec2.internal:9092 --topic rsvp_pan --group group-pan
```



## Write 5 Spark Structured Streaming Job

```
Write Spark Streaming jobs to process data from Kafka.
```
```
1. Save events in HDFS in text(json) format. 
   Use "kafka" source and "file" sink. 
   Set outputMode to "append".
```

```
spark-shell
spark.conf.set("spark.sql.shuffle.partitions",2)
```
```
val df1=spark.readStream
.format("kafka")
.option("kafka.bootstrap.servers","ip-172-31-94-165.ec2.internal:9092")
.option("subscribe","rsvp_pan")
.option("startingOffsets","earliest")
.option("failOnDataLoss","false").load()
df1.isStreaming
```
```
val df2=df1.select("value")
```
```
import org.apache.spark.sql.types._
val df3=df2.withColumn("value",col("value").cast(StringType))
df3.printSchema()
df3.writeStream.format("console").start()
```
```
import org.apache.spark.sql.streaming._
df3.writeStream.trigger(Trigger.ProcessingTime("60 seconds")).format("json").option("path","/user/pan/DE_project/json").option("checkpointLocation","/user/pan/DE_project/json/checkpoint").outputMode("append").start
```


```
2. Save events in HDFS in parquet format with schema. Use "kafka" source and "file" sink. Set outputMode to "append".
```

Create test.json with one record of https://stream.meetup.com/2/rsvps
```
val test=spark.read.format("json").option("inferSchema",true).load("/user/pan/DE_project/test.json")
test.printSchema
```
Methond 1 - read from static data
```
val df4=spark.readStream.schema(test.schema).option("maxFilesPerTrigger",2).json("/user/pan/DE_project/json/*.json")
df4.printSchema()
df4.isStreaming
import org.apache.spark.sql.streaming.Trigger
df4.writeStream.trigger(Trigger.ProcessingTime("60 seconds")).format("parquet").option("path","/user/pan/DE_project/parquet").option("checkpointLocation","/user/pan/DE_project/parquet/checkpoint").outputMode("append").start
```
Method 2 - read from kafka source
```
val df5=df3.select(from_json(col("value"), test.schema).as("data"))
df5.printSchema()
df5.isStreaming
val df6=df5.select(col("data.*"))
df6.printSchema()
import org.apache.spark.sql.streaming.Trigger
df6.writeStream.trigger(Trigger.ProcessingTime("60 seconds")).format("parquet").option("path","/user/pan/DE_project/parquet2").option("checkpointLocation","/user/pan/DE_project/parquet2/checkpoint").outputMode("append").start
df6.writeStream.format("console").start
```
[from_json example1](https://sparkbyexamples.com/spark/spark-parse-json-from-text-file-string/)
[from_json example2](https://sparkbyexamples.com/spark/spark-streaming-with-kafka/)



```
3. Show how many events are received, display in a 2-minute tumbling window. 
Show result at 1-minute interval. 
Use "kafka" source and "console" sink. 
Set outputMode to "complete".
```
```
df1.printSchema
val df8=df1.select("timestamp")
df8.printSchema
df8.writeStream.format("console").option("truncate","false").start
```
Display in a 2-minute tumbling window
```
df8.groupBy(window(col("timestamp"),"2 minutes")).count().orderBy("window").writeStream.trigger(Trigger.ProcessingTime("60 seconds")).queryName("df8").format("console").outputMode("complete").option("truncate", "false").start()
```
![1](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/tumbling_window.png)







```
4. Show how many events are received for each country, display it in a slidingwindow (set windowDuration to 3 minutes and slideDuration to 1 minutes). 
Show result at 1-minute interval. Use "kafka" source and "console" sink.
Set outputMode to "complete".
```

```
df1.printSchema
val df9=df1.select("timestamp","value")
```
```
import org.apache.spark.sql.types._
val df10=df9.withColumn("value",col("value").cast(StringType))
df10.printSchema()
df10.writeStream.format("console").start()
```
```
val df11=df10.select(col("timestamp"),from_json(col("value"), test.schema).as("data"))
df11.printSchema()
df11.isStreaming
val df12=df11.select(col("timestamp"),col("data.*"))
df12.printSchema()
```
```
df12.groupBy(window(col("timestamp"),"3 minutes","1 minutes"),col("group.group_country")).count().orderBy("window").writeStream.queryName("df12").trigger(Trigger.ProcessingTime("60 seconds")).format("console").outputMode("complete").option("truncate", "false").start()
```
![2](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/sliding_window.png)


```
5. Use impala to create a KUDU table. 
Do dataframe transformation to extract information and write to the KUDU table. 
Use "kafka" source and "kudu" sink.
create table if not exists rsvp_db.rsvp_kudu_<your-id>
(
	rsvp_id         bigint primary key,
	member_id       bigint,
	member_name     string,
	group_id        bigint,
	group_name      string,
	group_city      string,
	group_country   string,
	event_name      string,
	event_time      bigint
)
PARTITION BY HASH PARTITIONS 2
STORED AS KUDU;
Include following kudu-specific code in writeStream:
.format("kudu")
.option("kudu.master", "the actual kudu master")
.option("kudu.table", "impala::rsvp_db.rsvp_kudu_<your-id>")
.option("kudu.operation", "upsert")
Use impala query to verify that the streaming job is inserting data correctly.
```
```
create table if not exists rsvp_db.rsvp_kudu_pan
(
	rsvp_id         bigint primary key,
	member_id       bigint,
	member_name     string,
	group_id        bigint,
	group_name      string,
	group_city      string,
	group_country   string,
	event_name      string,
	event_time      bigint
)
PARTITION BY HASH PARTITIONS 2
STORED AS KUDU;
```
```
ssh pan@54.86.193.122 
ssh pan@ip-172-31-92-98.ec2.internal
./spark-shell --packages "org.apache.kudu:kudu-spark2_2.11:1.10.0-cdh6.3.2" --repositories "https://repository.cloudera.com/artifactory/cloudera-repos"
```
```
spark-shell
import org.apache.spark.sql.streaming.Trigger
```
```
val df7=df6.na.drop()
df7.writeStream.trigger(Trigger.ProcessingTime("60 seconds")).format("kudu").option("kudu.master", "ip-172-31-89-172.ec2.internal").option("kudu.table", "impala::rsvp_db.rsvp_kudu_pan").option("kudu.operation", "upsert").option("path","/user/pan/DE_project/kudu").option("checkpointLocation","/user/pan/DE_project/kudu/checkpoint").outputMode("append").start
```
Use impala query to verify that the streaming job is inserting data correctly.
```
SELECT * FROM rsvp_db.rsvp_kudu_pan LIMIT 10;
```



## Convert epoch_seconds to timestamp
Define the unit of time is milliseconds by https://www.epochconverter.com/
```
df6.select("event.time").writeStream.format("console").start
```
1 millisecond=0.001 seconds
```
import org.apache.spark.sql.functions._
val df6_seconds=df6.withColumn("time_seconds",col("event.time")/1000)
df6_seconds.select("time_seconds").writeStream.format("console").start
df6_seconds.printSchema
```
Convert epoch_seconds to timestamp
```
val df6_withEventTime=df6_seconds.withColumn("event_time",col("time_seconds").cast("timestamp"))
df6_withEventTime.printSchema
df6_withEventTime.select("event_time").writeStream.format("console").start
```


