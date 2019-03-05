package uk.co.brayan.fraudDetection

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.classification.RandomForestClassificationModel
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Assign
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies._
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.TopicPartition
import com.datastax.spark.connector.cql.CassandraConnector

import scala.collection.mutable.Map

object streamingApplication extends App with facilitator {
  import spark.implicits._

  // Loading customerDF
  val customerDf = cassandraDriver.readFromCassandra(spark,config.keyspace, config.customerTable)

  /* Loading  Preprocessing Model and Random Forest Model saved by daily batch job */
  val preprocessorTransformerModel = PipelineModel.load(config.preprocessorTransformerModelPath)
  val randomForestModel = RandomForestClassificationModel.load(config.modelPath)

  val connector: CassandraConnector = CassandraConnector(sparkConf)

  // Broadcasting read-only variables to the cluster(spark nodes)
  val broadcastMap = spark.sparkContext.broadcast(Map("keyspace" -> config.keyspace,
    "fraudTable" -> config.fraudTransactionTable,
    "nonFraudTable" -> config.nonFraudTransactionTable,
    "kafkaOffsetTable" -> config.kafkaOffsetTable))

  val topics = List(config.kafkaParams("topic")).toSet

  // setting properties for Kafka Consumer
  val kafkaParams:Map[String, String]=Map(
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> config.kafkaParams("bootstrap.servers"),
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> config.kafkaParams("key.deserializer.class"),
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> config.kafkaParams("value.deserializer.class"),
    ConsumerConfig.GROUP_ID_CONFIG -> config.kafkaParams("group.id"),
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> config.kafkaParams("auto.offset.reset")
  )

  // We maintain information regarding offset for each partition on a table in a Cassandra keyspace to further ensure exactly once processing.

  /* A method used to read stored information about partitions and offsets
  * @input : topicName, name of keyspace where partition and offset information is stored, table name
  * @output: an Array of tuple in the format of (TopicPartition object, offset)
  */
  def getOffsets(topic:String,keySpace:String, table:String): Array[(TopicPartition, Long)] = {
    val offsetsDf = cassandraDriver.readFromCassandra(spark,keySpace,table) // partition, offset

    offsetsDf.rdd.collect().map(raw=>{
      (new TopicPartition(topic, raw.getAs[Int]("partition"))->raw.getAs[Long]("offset"))
    })
  }

  val storedOffsets: Array[(TopicPartition, Long)] = getOffsets(config.kafkaParams("topic"),config.keyspace, config.kafkaOffsetTable)

  // creating a StreamingContext
  val ssc: StreamingContext = new StreamingContext(spark.sparkContext, Duration(config.batchInterval))

  // Creating a InputDStream[ConsumerRecord[String, String]]
  val dStream  = storedOffsets.isEmpty match {
    case true => {
      KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))
    }
    case false => {
      KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent,Assign[String, String](storedOffsets.toMap.keys.toList, kafkaParams, storedOffsets.toMap))
    }
  }


  // Now each ConsumerRecord[String, String] is mapped to (value, partition,offset)
  val transactionStream: DStream[(String, Int, Long)] =  dStream.map(cr => (cr.value(), cr.partition(), cr.offset()))

  // function passed to foreachRDD method will be executed per each RDD in the DStream object.
  transactionStream.foreachRDD(rdd => {

    if (!rdd.isEmpty()) {

      // converting each RDD into a DataFrame.
      val kafkaTransactionDf: DataFrame = rdd.toDF("transaction", "partition", "offset")
        .withColumn("transaction", from_json(col("transaction"),new StructType()
          .add("card_number", StringType,true)
          .add("transaction_number", StringType, true)
          .add("transaction_time", TimestampType, true)
          .add("category", StringType, true)
          .add("merchant_name", StringType, true)
          .add("amount", StringType, true)
          .add("merchant_latitude", StringType, true)
          .add("merchant_longitude", StringType, true)))
        .select("transaction.*", "partition", "offset")
        .withColumn("amount", lit($"amount") cast (DoubleType))
        .withColumn("merchant_latitude", lit($"merchant_latitude") cast (DoubleType))
        .withColumn("merchant_longitude", lit($"merchant_longitude") cast (DoubleType))
        .withColumn("transaction_time", lit($"transaction_time") cast (TimestampType))

      // Setting the maximum size in bytes for a table that will be broadcast to all worker nodes when performing a join.
      spark.sqlContext.sql("SET spark.sql.autoBroadcastJoinThreshold = 52428800")


      val distanceUdf = udf(getDistance _)
      val amendedTransactionDf = kafkaTransactionDf.join(broadcast(customerDf), Seq("card_number"))
      .withColumn("distance_from_customer_address", lit(round(distanceUdf($"latitude", $"longitude", $"merchant_latitude", $"merchant_longitude"), 2)))
      .withColumnRenamed("customer_age","age_at_transaction")
        .select("card_number","category","merchant_name","distance_from_customer_address","amount","age_at_transaction","transaction_number","transaction_time",
        "merchant_latitude","merchant_longitude","partition","offset")


      val featureTransactionDF = preprocessorTransformerModel.transform(amendedTransactionDf)
      // Creating a DataFrame that contains prediction information.
      val predictionDF: DataFrame = randomForestModel.transform(featureTransactionDF).withColumnRenamed("prediction", "is_fraud")

      // predictionDf may be distributed among partitions. For each partition passed function will be executed.
      predictionDF.foreachPartition((partition: Iterator[Row]) => {

        // Retrieving broadcast information
        val keyspace = broadcastMap.value("keyspace")
        val fraudTable = broadcastMap.value("fraudTable")
        val nonFraudTable = broadcastMap.value("nonFraudTable")
        val kafkaOffsetTable = broadcastMap.value("kafkaOffsetTable")

        // passed function will be executed per each session.
        connector.withSessionDo(session => {

          // creating PreparedStatements
          val preparedStatementFraud = session.prepare(cassandraDriver.cqlTransactionPrepare(keyspace, fraudTable))
          val preparedStatementNonFraud = session.prepare(cassandraDriver.cqlTransactionPrepare(keyspace, nonFraudTable))
          val preparedStatementOffset = session.prepare(cassandraDriver.cqlOffsetPrepare(keyspace, kafkaOffsetTable))

          val partitionOffset: Map[Int, Long] = Map.empty

          // passed function will be executed for each Row.
          partition.foreach(record => {
            val isFraud = record.getAs[Double]("is_fraud")

            // saving in appropriate table based on the prediction.
            if (isFraud == 1.0) {
              session.execute(cassandraDriver.cqlTransactionBind(preparedStatementFraud, record))
            }
            else if (isFraud == 0.0) {
              session.execute(cassandraDriver.cqlTransactionBind(preparedStatementNonFraud, record))
            }

            // maintaining offset information
            val kafkaPartition = record.getAs[Int]("partition")
            val offset = record.getAs[Long]("offset")
            partitionOffset.get(kafkaPartition) match {
              case None => partitionOffset.put(kafkaPartition, offset)
              case Some(currentMaxOffset) => {
                if (offset > currentMaxOffset)
                  partitionOffset.update(kafkaPartition, offset)
              }

            }

          })

          partitionOffset.foreach(t => {
            // Binding and executing prepared statement for Offset Table
            session.execute(cassandraDriver.cqlOffsetBind(preparedStatementOffset, t))
          })


        })
      })

    }
  })

  ssc.start()
  ssc.awaitTermination()

}
