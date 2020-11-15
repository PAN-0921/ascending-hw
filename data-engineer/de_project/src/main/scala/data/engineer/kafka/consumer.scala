package data.engineer.kafka

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.time.Duration
import java.util.{Arrays, Properties}


object consumer {

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      println("Usage MeetupRsvpConsumerScala <Kafka_bootstrap_servers> <kafka_topic> <consumer-group-id>")
      return
    }

    val servers = args(0)
    val kafka_topic = args(1)
    val group = args(2)

    println("bootStrapServers = " + servers + ", kafka_topic = " + kafka_topic)

    val props: Properties = new Properties
    props.put("bootstrap.servers", servers)
    props.put("group.id", group)
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "1000")
    props.put("session.timeout.ms", "30000")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)


    consumer.subscribe(Arrays.asList(kafka_topic))
    System.out.println("Subscribed to topic " + kafka_topic)
    val i = 0

    while (true) {
      val records : ConsumerRecords[String, String] = consumer.poll(Duration.ofMillis(100))
      import scala.collection.JavaConversions._
      for (record : ConsumerRecord[String, String] <- records) {
        printf("offset = %d, key = %s, value = %s\n", record.offset, record.key, record.value)
      }
    }
  }
}

/*
test:
kafka-console-consumer --bootstrap-server localhost:9092 --topic meetup_rsvp
*/

//kafka-console-consumer --bootstrap-server ip-172-31-89-11.ec2.internal:9092 --topic rsvp_pan --group group-pan