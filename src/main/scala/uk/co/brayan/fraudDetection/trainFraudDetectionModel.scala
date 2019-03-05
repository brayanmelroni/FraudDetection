package uk.co.brayan.fraudDetection


import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage}
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.linalg.Vector

/**
  * This object is used to create a ML model on daily basis.
  */
object trainFraudDetectionModel extends App with facilitator{

  val requiredColumns = Seq("card_number","category","merchant_name","distance_from_customer_address","amount","age_at_transaction")
  // loading fraudulent transactions into a DataFrame
  val fraudTransactionDf = cassandraDriver.readFromCassandra(spark,config.keyspace, config.fraudTransactionTable).select(requiredColumns.head, requiredColumns .tail: _*)

  // loading non-fraudulent transactions into a DataFrame
  val nonFraudTransactionDf = cassandraDriver.readFromCassandra(spark,config.keyspace, config.nonFraudTransactionTable).select(requiredColumns.head, requiredColumns .tail: _*)


  /* A method used to create a Pipeline model after applying StringIndexer and One Hot encoding
  * @input : a DataFrame, a column Name that don't need to be considered when creating the model. Later argument is optional.
  * @output: a PipelineModel object
  */
  def getPreprocessorTransformerModel(df:DataFrame, labelCol:String=""):PipelineModel = {

    val numericColumns: ArrayBuffer[String] = new mutable.ArrayBuffer[String]()

    val strIndexOneHotStages: Seq[PipelineStage] = df.columns.toSeq.filter(_!= labelCol).flatMap(column => {
      val dataTypes =  df.select(column).dtypes.map(_._2)
      if(dataTypes(0) == "StringType") {
        numericColumns += s"${column}_encoded"
        Seq(new StringIndexer().setInputCol(column).setOutputCol(s"${column}_indexed").setHandleInvalid("keep"), new OneHotEncoder().setInputCol(s"${column}_indexed").setOutputCol(s"${column}_encoded"))
      }
      else{
        numericColumns += column
        Seq()
      }
    })


    val vectorAssembler = new VectorAssembler()
    vectorAssembler.setInputCols(numericColumns.toArray).setOutputCol("features")

    val allStages: Seq[PipelineStage] = strIndexOneHotStages ++ Seq(vectorAssembler)
    val pipeline: Pipeline = new Pipeline().setStages(allStages.toArray)
    val preprocessorTransformerModel: PipelineModel = pipeline.fit(df)
    preprocessorTransformerModel
  }

  // Creating a Pipeline Model  using  all the transactions.
  val pipelineModel: PipelineModel =getPreprocessorTransformerModel(nonFraudTransactionDf.union(fraudTransactionDf))
  // Saving the Pipeline Model
  pipelineModel.save(config.preprocessorTransformerModelPath)

  // transforming the fraudTransaction DataFrame using created pipelineModel
  val fraudFeaturesDf = pipelineModel.transform(fraudTransactionDf).withColumn("label", lit(1)).select("features","label")


  /* A method used to apply KMeans clustering on a DataFrame and return cluster points
  * @input : a DataFrame,  number of cluster points, maximum number of Iterations
  * @output: a List of Vectors . Each Vector corresponds to a cluster point.
  */
  def getKMeansClusterPoints(df:DataFrame,numClusterPoints:Int, maxIterations: Int): List[Vector] = {
    val kMeans: KMeans = new KMeans().setK(numClusterPoints).setMaxIter(maxIterations)
    val kMeansModel = kMeans.fit(df)
    kMeansModel.clusterCenters.toList
  }

  val fraudCount= fraudFeaturesDf.count().toInt
  val nonFraudFeaturesDf = pipelineModel.transform(nonFraudTransactionDf).select("features")
  import spark.implicits._

  /* 1. The number of rows in nonFraudFeaturesDf > number of rows in FraudFeaturesDf
     2. So KMeans Clustering is applied to nonFraudFeaturesDf and number of cluster points equal to the size of FraudFeaturesDf are found.
     3. A new DataFrame was created using cluster points.
     4. This new DataFrame is combined with FraudFeaturesDf via Union operator.
     5. This whole operation is done to create a balance between Fraud transaction data and non fraud transaction data.
  */
  val nonFraudFeaturesBalancedDf  = getKMeansClusterPoints(nonFraudFeaturesDf,fraudCount,30).map(v => (v, 0)).toDF("features", "label")
  val finalFeaturesDf = fraudFeaturesDf.union(nonFraudFeaturesBalancedDf)


  /* A method used to create a Random Forest Model
  * @input : a DataFrame, name of features column, name of label column, maximum number of bins
  * @output: a RandomForestClassificationModel
  */
  def getRandomForestModel(df:DataFrame,featuresCol:String,lblCol:String,maxBins:Int):RandomForestClassificationModel = {
    val Array(training, test) = df.randomSplit(Array(0.7, 0.3))
    val randomForestEstimator = new RandomForestClassifier().setLabelCol(lblCol).setFeaturesCol(featuresCol).setMaxBins(maxBins)
    val randomForestModel = randomForestEstimator.fit(training)

    val predictions = randomForestModel.transform(test)
    val accEval = new MulticlassClassificationEvaluator().setLabelCol(lblCol)
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    println(s"total test data count is ${predictions.count()}")
    println(s"Accuracy : ${accEval.evaluate(predictions)}")
    randomForestModel
  }

  // Random Forest Model is created and saved.
  val randomForestModel: RandomForestClassificationModel = getRandomForestModel(finalFeaturesDf,"features","label",700)
  randomForestModel.save(config.modelPath)

}
