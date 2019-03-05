package uk.co.brayan.fraudDetection



import com.datastax.driver.core.{BoundStatement, PreparedStatement}
import org.apache.spark.sql._
import java.sql.Timestamp

object cassandraDriver {

  /*
  * A method used to read a table in a keyspace and create a DataFrame
  * @input : sparkSession object, keyspace name and a table name
  * @output: a DataFrame  that represents the table
  */
  def readFromCassandra(spark:SparkSession,keySpace:String, table:String):DataFrame= {
    spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> keySpace, "table" -> table))
      .load()
  }

  /*
  * A method used to append data in a DataFrame to a table
  * @input : DataFrame object, keyspace  name and a table name
  * @output: Unit
  */
  def appendDataToCassandra(df:DataFrame,keyspace:String,table:String): Unit ={
    df.write
      .format("org.apache.spark.sql.cassandra")
      .mode("append")
      .options(Map("keyspace" -> keyspace, "table" -> table))
      .save()
  }

  /*
  * A method used to generate a CQL insert statement, to store data into a table with following columns:
  * "card_number","transaction_time","transaction_number","category","merchant_name","amount","merchant_latitude","merchant_longitude","distance_from_customer_address","age_at_transaction"
  * @input : name of the keyspace and table
  * @output: a String representing insert statement
  */
  def cqlTransactionPrepare(keyspace:String, table:String): String = {
    s"""
     insert into $keyspace.$table ("card_number","transaction_time","transaction_number","category","merchant_name","amount","merchant_latitude","merchant_longitude","distance_from_customer_address","age_at_transaction")
     values(
       ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )"""
  }

  /*
  * A method used to generate a CQL insert statement, to store data into a table with columns: "partition","offset"
  * @input : name of the keyspace and table
  * @output: a String representing insert statement
  */
  def cqlOffsetPrepare(keyspace:String, table:String) = {
    s"""
     insert into $keyspace.$table (
       "partition",
       "offset"
     )
     values(
       ?, ?
        )"""
  }

  /*
  * A method used to create an executable bound statement that can be executed by a session
  * @input : a Prepared statement object and a Row object with following fields:
  * "card_number","transaction_time","transaction_number","category","merchant_name","amount","merchant_latitude","merchant_longitude","distance_from_customer_address","age_at_transaction"
  * @output: a corresponding BoundStatement object
  */
  def cqlTransactionBind(prepared: PreparedStatement, record:Row): BoundStatement ={
    val bound: BoundStatement = prepared.bind()
    bound.setString("card_number", record.getAs[String]("card_number"))
    bound.setTimestamp("transaction_time", record.getAs[Timestamp]("transaction_time"))
    bound.setString("transaction_number", record.getAs[String]("transaction_number"))
    bound.setString("category", record.getAs[String]("category"))
    bound.setString("merchant_name", record.getAs[String]("merchant_name"))
    bound.setDouble("amount", record.getAs[Double]("amount"))
    bound.setDouble("merchant_latitude", record.getAs[Double]("merchant_latitude"))
    bound.setDouble("merchant_longitude", record.getAs[Double]("merchant_longitude"))
    bound.setDouble("distance_from_customer_address", record.getAs[Double]("distance_from_customer_address"))
    bound.setInt("age_at_transaction", record.getAs[Int]("age_at_transaction"))
    bound
  }

  /*
  * A method used to create an executable bound statement that can be executed by a session
  * @input : a Prepared statement object and a tuple(Int,Long)
  * @output: a BoundStatement object with "partition","offset" values
  */
  def cqlOffsetBind(prepared: PreparedStatement, record:(Int, Long)): BoundStatement ={
    val bound = prepared.bind()
    bound.setInt("partition",record._1)
    bound.setLong("offset", record._2)
    bound
  }

}