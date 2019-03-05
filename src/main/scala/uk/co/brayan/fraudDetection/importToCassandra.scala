package uk.co.brayan.fraudDetection

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
  * This object is used to store customer and transaction data from CSV files created before the system was developed into Cassandra.
  */
object importToCassandra extends App with facilitator{

  // reading customer data to a DataFrame, adding a new column to represent customer age and saving DataFrame to Cassandra
  val customerDf:DataFrame = readCSV(config.customerCSVFilePath,schemas.customerSchemaForCSV)
    .withColumn("customer_age", (datediff(current_date(), to_date(col("date_of_birth"))) / 365).cast(IntegerType))
  cassandraDriver.appendDataToCassandra(customerDf,config.keyspace,config.customerTable)

  // a user defined function
  val distanceUdf = udf(getDistance _)

  // reading transaction data into a DataFrame, adding a column to represent transaction time, dropping transaction_date  and unix_time columns
  val transactionDf:DataFrame = readCSV(config.transactionsCSVFilePath,schemas.transactionSchemaForCSV)
    .withColumn("transaction_date", split(col("transaction_date"), "T").getItem(0))
    .withColumn("transaction_time", concat_ws(" ", col("transaction_date"), col("transaction_time")))
    .withColumn("transaction_time", to_timestamp(col("transaction_time"), "YYYY-MM-dd HH:mm:ss") cast (TimestampType))
    .drop("Transaction_date","unix_time")

  // joining transaction DataFrame and customer DataFrame based on card_number, adding two columns to represent distance from customer address, age at transaction
  // Then dropping columns: latitude, longitude, date_of_birth
  val joinedTransactionDf = transactionDf.join(customerDf.select("card_number","latitude","longitude","date_of_birth"), Seq("card_number"))
    .withColumn("distance_from_customer_address", lit(round(distanceUdf(col("latitude"), col("longitude"), col("merchant_latitude"), col("merchant_longitude")), 2)))
    .withColumn("age_at_transaction", (datediff(to_date(col("transaction_time")), to_date(col("date_of_birth"))) / 365).cast(IntegerType))
    .drop("latitude","longitude","date_of_birth")

  joinedTransactionDf.persist()

  // filtering fraudulent transactions
  val fraudDf = joinedTransactionDf.filter("is_fraud  == 1").drop("is_fraud")

  // filtering non-fraudulent transactions
  val nonFraudDf = joinedTransactionDf.filter("is_fraud  == 0").drop("is_fraud")

  // saving DataFrames associated with fraudulent and non fraudulent transactions to Cassandra
  cassandraDriver.appendDataToCassandra(fraudDf,config.keyspace,config.fraudTransactionTable)
  cassandraDriver.appendDataToCassandra(nonFraudDf,config.keyspace,config.nonFraudTransactionTable)

}
