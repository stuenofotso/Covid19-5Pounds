package covid19

import java.text.SimpleDateFormat

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.{GBTRegressor, GeneralizedLinearRegression}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.functions._
import org.elasticsearch.hadoop.cfg.ConfigurationOptions
import org.elasticsearch.hadoop.serialization.Parser
import org.elasticsearch.spark.sql.{EsSparkSQL, ScalaRowValueReader}








/** Main class */
object ForecastGLR {

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


    val elasticIndex = "covid19_raw_data"
    val url = "ec2-3-135-203-125.us-east-2.compute.amazonaws.com:9200,ec2-13-58-53-1.us-east-2.compute.amazonaws.com:9200,ec2-18-220-232-235.us-east-2.compute.amazonaws.com:9200"

    println(s"Loading index ${elasticIndex} at : ${url} ...")

    val df = EsSparkSQL.esDF(spark.sqlContext,
      Map(
        ConfigurationOptions.ES_RESOURCE_READ -> elasticIndex,
        ConfigurationOptions.ES_INDEX_READ_MISSING_AS_EMPTY -> "true",
        ConfigurationOptions.ES_NET_HTTP_AUTH_USER->"elastic",
        ConfigurationOptions.ES_NET_HTTP_AUTH_PASS->"W|Hed%/E$]=(",
        ConfigurationOptions.ES_NODES_WAN_ONLY->"true",
        ConfigurationOptions.ES_NODES->url,
        "es.read.field.include" -> "Country,Date,Cases,Status",
        // Option to specify a serialization reader
        ConfigurationOptions.ES_SERIALIZATION_READER_VALUE_CLASS -> classOf[SpecificBasicDateTimeReader].getCanonicalName
      ))


    println("df.printSchema()")
    df.printSchema()

    df.show(5)

    //val groupedDf =  df.groupBy($"Country",  window($"Date", "1 day")).sum("Cases").alias("Cases").withColumn("Date",window($"Date", "1 day"))
    val groupedDf = df.groupBy($"Country", $"Date").agg(expr("sum(case when Status = 'confirmed'then Cases else 0 end) as Confirmed_Cases")
      , expr("sum(case when Status = 'recovered'then Cases else 0 end) as Recovered_Cases")
      , expr("sum(case when Status = 'deaths'then Cases else 0 end) as Dead_Cases"))
      .withColumn("Date", dayofyear(col("Date"))+(lit(365) * year(col("Date")))) //timestamp to Integer


    val indexedDf = new StringIndexer()
      .setInputCol("Country")
      .setOutputCol("CountryIndex")
      .fit(groupedDf)
      .transform(groupedDf)


    println("indexedDf.printSchema()")
    indexedDf.printSchema()


    val readyDf = //new VectorAssembler().setInputCols(Array("CountryIndex", "Date", "Recovered_Cases","Dead_Cases"))
              new VectorAssembler().setInputCols(Array("CountryIndex", "Date"))
                      .setOutputCol("features")
                      .transform(indexedDf)
                      .withColumn("label", $"Confirmed_Cases")
                      .select("features", "label").cache()

    println("readyDf.printSchema()")
    readyDf.printSchema()



    // Train a GBT model.

    val glr = new GeneralizedLinearRegression()
      .setLink("identity")
      .setLabelCol("label")
      .setFeaturesCol("features")




    // Split the data into train and test
    val splits = readyDf.randomSplit(Array(0.9, 0.1), seed = 1234L)
    val trainData = splits(0)
    val testData = splits(1)



    /*val  paramGrid = new ParamGridBuilder()
      .addGrid(gbt.maxBins, Array(250, 300))
      .addGrid(gbt.maxDepth, Array(10, 20, 30))
      .build()*/

    val  paramGrid = new ParamGridBuilder()
      .addGrid(glr.regParam, Array(0.2,0.3,0.5,0.7,0.9))
      .addGrid(glr.maxIter, Array(10,20,30))
      .addGrid(glr.fitIntercept, Array(true, false))
      .addGrid(glr.family, Array("gaussian", "tweedie","gaussian", "poisson"))
      .build()


    // Chain indexer and GBT in a Pipeline.
//    val pipeline = new Pipeline()
//      .setStages(Array(gbt))

    val cv = new CrossValidator()
      .setEstimator(glr)
      .setEvaluator(new RegressionEvaluator())
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(3)
      .setParallelism(3)

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

    //val gbtModel = model.stages(1).asInstanceOf[GBTRegressionModel]
    println(s"Learned regression GLR model:\n ${model.toString()}")

    model.save("models/GLRRegressionModel.ml")
  }


}