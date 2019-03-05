package uk.co.brayan.fraudDetection

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}


import scala.collection.mutable.Map

/**
  * This object is used to load configurations from an external file.
  */
object config {

  val applicationConf:Config = ConfigFactory.parseFile(new File("src/main/resources/spark.conf"))

  /* Configurations related to external CSV files:
  *  There are two csv files. One containing customer information and another containing transaction information.
  *  These files were generated before the system is implemented.
  */
  val customerCSVFilePath = applicationConf.getString("config.spark.customer.datasource")
  val transactionsCSVFilePath = applicationConf.getString("config.spark.transaction.datasource")

  // Configurations related to ML models
  val preprocessorTransformerModelPath = applicationConf.getString("config.spark.model.preprocessing.path")
  val modelPath = applicationConf.getString("config.spark.model.path")

  // SparkStreaming configurations
  val checkpointLocation = applicationConf.getString("config.spark.checkpoint")
  val batchInterval = applicationConf.getString("config.spark.batch.interval").toLong
  val shutdownMarker = applicationConf.getString("config.spark.shutdownMarker")


  // Configurations related to Cassandra database
  val cassandraHost = applicationConf.getString("config.cassandra.host")
  val keyspace = applicationConf.getString("config.cassandra.keyspace")
  val fraudTransactionTable = applicationConf.getString("config.cassandra.table.fraud.transaction")
  val nonFraudTransactionTable = applicationConf.getString("config.cassandra.table.non.fraud.transaction")
  val customerTable = applicationConf.getString("config.cassandra.table.customer")
  val kafkaOffsetTable = applicationConf.getString("config.cassandra.table.kafka.offset")


  // Configurations related to Kafka Consumer
  val kafkaParams: Map[String, String] = Map.empty
  kafkaParams.put("topic", applicationConf.getString("config.kafka.topic") /*"credit_card_transaction"*/)
  kafkaParams.put("enable.auto.commit",applicationConf.getString("config.kafka.enable.auto.commit"))
  kafkaParams.put("group.id", applicationConf.getString("config.kafka.group.id"))
  kafkaParams.put("bootstrap.servers", applicationConf.getString("config.kafka.bootstrap.servers"))
  kafkaParams.put("auto.offset.reset", applicationConf.getString("config.kafka.auto.offset.reset"))
  kafkaParams.put("key.deserializer.class", applicationConf.getString("config.kafka.key.deserializer"))
  kafkaParams.put("value.deserializer.class", applicationConf.getString("config.kafka.value.deserializer"))

}
