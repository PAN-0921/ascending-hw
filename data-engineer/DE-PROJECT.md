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




## Create Kafka Data Source

```
Use meetup RSVP event as the Kafkadata source. Ingest meetup_rsvp event to the Kafka topic, use kafka-console-consumer to verify it.
Meetup provides a streaming API for retrieving the RSVP event.
https://stream.meetup.com/2/rsvps
```
```
Hint:
The easiest way is to use kafka-console-producer
Bonus:
Write a Kafka producerand a Kafka consumer using Scala, or Java, or Python to produce/consumeevents to/from the Kafka topic.
```

## Solution 


## Write 5 Spark Structured Streaming Job

```
Write Spark Streaming jobs to process data from Kafka.
```
```
1. Save events in HDFS in text(json) format. Use "kafka" source and "file" sink. Set outputMode to "append".
```
```
2. Save events in HDFS in parquet format with schema. Use "kafka" source and "file" sink. Set outputMode to "append".
```
```
3. Show how many events are received, display in a 2-minute tumbling window. 
Show result at 1-minute interval. 
Use "kafka" source and "console" sink. 
Set outputMode to "complete".
```
```
4. Show how many events are received for each country, display it in a slidingwindow (set windowDuration to 3 minutes and slideDuration to 1 minutes). 
Show result at 1-minute interval. Use "kafka" source and "console" sink.
Set outputMode to "complete".
```
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

## Solution 



## Use YARN web console to monitor the job

## Solution 