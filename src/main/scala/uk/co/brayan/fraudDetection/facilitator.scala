package uk.co.brayan.fraudDetection

import org.apache.spark.SparkConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

// Used to store some common fields and methods used by other objects.
trait facilitator{

  val sparkConf = new SparkConf
  sparkConf.setMaster("local[*]")
    .set("spark.cassandra.connection.host", config.cassandraHost)
    .set("spark.sql.streaming.checkpointLocation", config.checkpointLocation)

  lazy val spark: SparkSession = SparkSession.builder
    .config(sparkConf)
    .getOrCreate()


  /*
  * A method used to read a CSV file and create a DataFrame
  * @input : csv file name, StructType object representing a schema for  CSV file
  * @output: a DataFrame
  */
  def readCSV(fileName:String, schema:StructType):DataFrame= {
    spark.read
      .option("header", true)
      .option("inferSchema",true)
      .schema(schema)
      .csv(fileName)
  }

  /*
  * A method used to find geo distance between two locations.
  * @input : latitude, longitude of two locations
  * @output: Geo distance between two locations
  */
  def getDistance (lat1:Double, lon1:Double, lat2:Double, lon2:Double) = {
    val r : Int = 6371 //Earth radius
    val latDistance : Double = Math.toRadians(lat2 - lat1)
    val lonDistance : Double = Math.toRadians(lon2 - lon1)
    val a : Double = Math.sin(latDistance / 2) * Math.sin(latDistance / 2) + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2)
    val c : Double = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
    val distance : Double = r * c
    distance
  }

}
