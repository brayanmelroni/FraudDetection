package uk.co.brayan.producer

import java.io.File
import java.nio.charset.Charset
import java.text.SimpleDateFormat
import java.util.{Date, Properties, TimeZone}

import com.google.gson.{Gson, JsonObject}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.csv.{CSVFormat, CSVParser, CSVRecord}
import org.apache.kafka.clients.producer._

import scala.collection.JavaConverters._
import scala.util.Random

/* This object is used to generate a real time stream of transaction data for testing purposes.
   In the production system, there will be a live stream of credit/debit card transactions.
*  One transaction corresponds to a JSON string.
*/
object TransactionProducer extends App{

  // Opening the CSV file, that is used to generate a stream of JSON strings.
  val applicationConf:Config = ConfigFactory.parseFile(new File(args(0)))
  val file = new File(applicationConf.getString("kafka.producer.file"))
  val csvParser: CSVParser = CSVParser.parse(file, Charset.forName("UTF-8"), CSVFormat.DEFAULT)

  val gson: Gson = new Gson
  val rand: Random = new Random

  // Setting properties for kafka producer, based on a given configuration file.
  val properties = new Properties()
  properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, applicationConf.getString("kafka.bootstrap.servers"))
  properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, applicationConf.getString("kafka.key.serializer"))
  properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, applicationConf.getString("kafka.value.serializer"))

  properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, applicationConf.getString("kafka.enable.idempotence.config"))
  properties.setProperty(ProducerConfig.ACKS_CONFIG, applicationConf.getString("kafka.acks"))
  properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE))
  properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,applicationConf.getString("kafka.max.in.flight.requests.per.connection"))

  val topic:String = applicationConf.getString("kafka.topic")
  val producer:KafkaProducer[String, String] = new KafkaProducer[String,String](properties)

  /*
  * A method used to create a JSON string from CSVRecord object
  * @input : CSVRecord object
  * @output: a String that contains a JSON representation of CSVRecord object.
  */
  def recordToJson(record:CSVRecord): String = {
    val obj: JsonObject = new JsonObject
    obj.addProperty(Transaction.card_number, record.get(0))
    obj.addProperty(Transaction.transaction_number, record.get(3))

    val isoFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    isoFormat.setTimeZone(TimeZone.getTimeZone("IST"));
    val date = new Date
    val timestamp = isoFormat.format(date)

    obj.addProperty(Transaction.transaction_time, timestamp)
    obj.addProperty(Transaction.category, record.get(7))
    obj.addProperty(Transaction.merchant, record.get(8))
    obj.addProperty(Transaction.amount, record.get(9))
    obj.addProperty(Transaction.merchant_latitude, record.get(10))
    obj.addProperty(Transaction.merchant_longitude, record.get(11))
    gson.toJson(obj)
  }

  /* For each CSVRecord in csvParser:
  *  1. A JSON representation of CSVRecord is created.
  *  2. A ProducerRecord object that contains JSON string is created.
  *  3. Each ProducerRecord object is sent to the topic.
  *
  *  A delay between sending two ProducerRecords is maintained via Thread.sleep method.
  */
  csvParser.asScala.foreach(record =>{
    val json = recordToJson(record)
    println(s"Transaction Record: ${json}")
    val producerRecord = new ProducerRecord[String, String](topic, json)

    producer.send(producerRecord,new Callback() {
      def onCompletion(recordMetadata:RecordMetadata, e:Exception) {
        if (e == null)
          println(s"Data was sent to partition: ${recordMetadata.partition()} . Current offset is: ${recordMetadata.offset()} . Timestamp @ the moment is: ${recordMetadata.timestamp()} .")
        else
          e.printStackTrace
      }
    })
    producer.flush
    Thread.sleep(rand.nextInt(3000 - 1000) + 1000)
  })

  producer.close
}
