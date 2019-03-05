package uk.co.brayan.fraudDetection

import org.apache.spark.sql.types._

/**
  * This object is used to generate schemas for reading external csv files generated before the system was implemented.
  */
object schemas extends App {
  // structType used to generate a schema for CSV file containing customer data
  val customerSchemaForCSV = new StructType()
    .add("card_number", StringType, true)
    .add("first_name", StringType, true)
    .add("last_name", StringType, true)
    .add("gender", StringType, true)
    .add("street", StringType, true)
    .add("city", StringType, true)
    .add("state", StringType, true)
    .add("zip", StringType, true)
    .add("latitude", DoubleType, true)
    .add("longitude", DoubleType, true)
    .add("job", StringType, true)
    .add("date_of_birth", TimestampType, true)

  // structType used to generate a schema for CSV file containing transaction data
  val transactionSchemaForCSV = new StructType()
    .add("card_number", StringType,true)
    .add("transaction_number", StringType, true)
    .add("transaction_date", StringType, true)
    .add("transaction_time", StringType, true)
    .add("unix_time", LongType, true)
    .add("category", StringType, true)
    .add("merchant_name", StringType, true)
    .add("amount", DoubleType, true)
    .add("merchant_latitude", DoubleType, true)
    .add("merchant_longitude", DoubleType, true)
    .add("is_fraud",IntegerType,true)
}

