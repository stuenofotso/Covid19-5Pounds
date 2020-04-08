package covid19

import java.text.SimpleDateFormat

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.GBTRegressor
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, LongType, StructField, StructType}
import org.elasticsearch.hadoop.cfg.ConfigurationOptions
import org.elasticsearch.hadoop.serialization.Parser
import org.elasticsearch.spark.sql.{EsSparkSQL, ScalaRowValueReader}







/** Main class */
object ForecastGBT {

  import org.apache.spark.sql.SparkSession

  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Covid19 Forecast")
      .config("spark.master", "local[4]")
      .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  // For implicit conversions like converting RDDs to DataFrames
  import spark.implicits._



  /** Main function */
  def main(args: Array[String]): Unit = {


    val groupedDf = spark.read.format("csv")
      .option("header","false")
      .option("inferSchema", "true")
      .load("grouped_coviddata.csv")
      .toDF("Country", "CountryCode", "Date",  "Confirmed", "Deaths", "Recovered")
        .na.fill(0)


    println("groupedDf.printSchema()")
    groupedDf.printSchema()
    groupedDf.show(10)


    val indexedDf = new StringIndexer()
      .setInputCol("Country")
      .setOutputCol("CountryIndex")
      .fit(groupedDf)
      .transform(groupedDf)




    val readyDf = //new VectorAssembler().setInputCols(Array("CountryIndex", "Date", "Recovered_Cases","Dead_Cases"))
              new VectorAssembler().setInputCols(Array("CountryIndex", "Date"))
                      .setOutputCol("features")
                      .transform(indexedDf)
                      .withColumn("label", $"Confirmed")
                      .select("features", "label").cache()




    // Split the data into train and test
    val splits = readyDf.randomSplit(Array(0.9, 0.1), seed = 1234L)
    val trainData = splits(0)
    val testData = splits(1)





    // Train a GBT model.
    val gbt = new GBTRegressor()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setMaxIter(500)


    /*val  paramGrid = new ParamGridBuilder()
      .addGrid(gbt.maxBins, Array(300,500,1000)) //best 300
      .addGrid(gbt.maxDepth, Array(4, 6, 8, 10,20,25,30)) //best 10
      .build()*/


    /*//Root Mean Squared Error (RMSE) on test data = 212.5890383960005
    val  paramGrid = new ParamGridBuilder()
      .addGrid(gbt.maxBins, Array(260,280,300))//260
      .addGrid(gbt.maxDepth, Array(4, 6, 8, 10,15))//10
      .build()
*/

    //Root Mean Squared Error (RMSE) on test data = 212.00467257169575
    /*val  paramGrid = new ParamGridBuilder()
      .addGrid(gbt.maxBins, Array(190, 200, 220,250)) //190
      .addGrid(gbt.maxDepth, Array(8, 10,12)) //10
      .build()*/

    val  paramGrid = new ParamGridBuilder()
      .addGrid(gbt.maxBins, Array(190))
      .addGrid(gbt.maxDepth, Array(10))
      .build()




    // Chain indexer and GBT in a Pipeline.
//    val pipeline = new Pipeline()
//      .setStages(Array(gbt))

    val cv = new CrossValidator()
      .setEstimator(gbt)
      .setEvaluator(new RegressionEvaluator())
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(3)
      .setParallelism(100)

    // Train model. This also runs the indexer.
    println("begin of model training...")
    val model = cv.fit(trainData)

    // Make predictions.
    val predictions = model.transform(testData)

    // Select example rows to display.
    predictions.select("prediction", "label", "features").show(5)

    // Select (prediction, true label) and compute test error.
    val evaluator = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("rmse")
    val rmse = evaluator.evaluate(predictions)
    println(s"Root Mean Squared Error (RMSE) on test data = $rmse")

    model.save("models/GBTRegressionModel_total_cov_v3.ml")

    //val gbtModel = model.stages(1).asInstanceOf[GBTRegressionModel]
    println(s"Learned regression GBT model:\n ${model.toString()}")

    println(s"Best params :\n ${model.bestModel.explainParams()}")


  }


}