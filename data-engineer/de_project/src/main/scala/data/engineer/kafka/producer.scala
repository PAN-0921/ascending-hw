package data.engineer.kafka

import java.io.{BufferedReader, IOException, InputStreamReader}
import java.util.concurrent.atomic.AtomicReference
import java.util.{Date, Properties}
import java.util.concurrent.{ExecutionException, Future}

import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClients
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}


object producer {
  var url = "http://stream.meetup.com/2/rsvps"

  @throws[InterruptedException]
  @throws[ExecutionException]
  private def displayRecordMetaData(record: ProducerRecord[String, String], future: Future[RecordMetadata]): Unit = {
    val recordMetadata = future.get
    println(
      String.format(
        "\n\t\t\tkey=%s, value=%s " +
          "\n\t\t\tsent to topic=%s part=%s off=%s at time=%s",
        record.key, record.value,
        recordMetadata.topic, recordMetadata.partition.toString,
        recordMetadata.offset.toString, new Date(recordMetadata.timestamp)
      )
    )
  }

  def createKafkaProducer(bootStrapServers: String): KafkaProducer[String, String] = {
    val props = new Properties
    props.put("bootstrap.servers", bootStrapServers)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new AtomicReference[KafkaProducer[String, String]](new KafkaProducer[String, String](props))
    producer.get
    //KafkaProducer<String, String> producer = new KafkaProducer<>(props);
    //return producer;
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.out.println("Usage MeetupRsvpProducerScala <Kafka_bootstrap_servers> <kafka_topic>")
      return
    }

    val bootStrapServers = args(0)
    val kafka_topic = args(1)
    println("bootStrapServers = " + bootStrapServers + ", kafka_topic = " + kafka_topic)

    val kafkaProducer = createKafkaProducer(bootStrapServers)

    val httpClient = HttpClients.createDefault

    try {
      val getRequest = new HttpGet(url)
      getRequest.addHeader("accept", "application/json")
      val response = httpClient.execute(getRequest)
      if (response.getStatusLine.getStatusCode != 200) {
        throw new RuntimeException("Failed : HTTP error code : " + response.getStatusLine.getStatusCode)
      }
      else {
        val br = new BufferedReader(new InputStreamReader(response.getEntity.getContent))
        var output : String = null
        println("Output from Server .... \n")

        output = br.readLine
        while ( output != null) {
          println(output)
          // when create a ProducerRecord, only topic and value are mandatory
          // ProducerRecord(String topic, Integer partition, Long timestamp, K key, V value)
          val record = new ProducerRecord[String, String](kafka_topic, output)
          //ProducerRecord<String, String> record = new ProducerRecord<>(kafka_topic, key, output);
          val sendRes = kafkaProducer.send(record)
          displayRecordMetaData(record, sendRes)
          output = br.readLine
        }
      }
    } catch {
      case e: IOException =>
        e.printStackTrace()
      case e: InterruptedException =>
        e.printStackTrace()
      case e: ExecutionException =>
        e.printStackTrace()
    } finally {
      httpClient.close()
      kafkaProducer.close()
    }

  }
}

/*
test:
kafka-console-consumer --bootstrap-server localhost:9092 --topic meetup_rsvp
*/

//curl https://stream.meetup.com/2/rsvps | kafka-console-producer --broker-list ip-172-31-94-165.ec2.internal:9092,ip-172-31-91-232.ec2.internal:9092,ip-172-31-89-11.ec2.internal:9092 --topic rsvp_pan