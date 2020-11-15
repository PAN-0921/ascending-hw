package data.engineer.training

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming._



object rsvp_streaming3 {

  private val log = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]) {


    //val streaming_app_config: RsvpConfig = RsvpConfigUtil.loadConfig(args(0))
    log.info("====================================================")
    // log.info(streaming_app_config.toString)
    log.info("====================================================")


    val spark = SparkSession.builder()
      .appName("deproject.spark_app_name")
      .config("spark.dynamicAllocation.enabled", "false") // default is true
      .config("spark.streaming.backpressure.enabled", "true")
      .config("spark.streaming.kafka.maxRatePerPartition", 100)
      .config("spark.sql.shuffle.partitions", 2) // def 200
      .getOrCreate()
    spark.sparkContext.setLogLevel("INFO")


    val df1 = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "ip-172-31-94-165.ec2.internal:9092")
      .option("subscribe", "rsvp_pan")
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "false")
      .load()


    val df8 = df1.select("timestamp")
    df8.writeStream.format("console").option("truncate", "false").start


    df8.withWatermark("timestamp","30 minutes")
      .groupBy(window(col("timestamp"), "2 minutes"))
      .count()
      .orderBy("window")
      .writeStream
      .trigger(Trigger.ProcessingTime("60 seconds"))
      .queryName("df8")
      .format("console")
      .outputMode("Complete")
      .option("truncate", "false")
      .start()
      .awaitTermination()

  }
}
