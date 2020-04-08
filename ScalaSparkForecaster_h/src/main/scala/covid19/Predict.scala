package covid19

import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.GBTRegressor
import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.elasticsearch.hadoop.cfg.ConfigurationOptions
import org.elasticsearch.spark.sql.EsSparkSQL




/** Main class */
object Predict {

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

  val url = "ec2-3-135-203-125.us-east-2.compute.amazonaws.com:9200,ec2-13-58-53-1.us-east-2.compute.amazonaws.com:9200,ec2-18-220-232-235.us-east-2.compute.amazonaws.com:9200,ec2-18-224-63-68.us-east-2.compute.amazonaws.com:9200"


  /** Main function */
  def main(args: Array[String]): Unit = {


    val elasticIndex = "covid19_raw_data"

    println(s"Loading index ${elasticIndex} at : ${url} ...")

    val df = EsSparkSQL.esDF(spark.sqlContext,
      Map(
        ConfigurationOptions.ES_RESOURCE_READ -> elasticIndex,
        ConfigurationOptions.ES_INDEX_READ_MISSING_AS_EMPTY -> "true",
        ConfigurationOptions.ES_NET_HTTP_AUTH_USER->"elastic",
        ConfigurationOptions.ES_NET_HTTP_AUTH_PASS->"W|Hed%/E$]=(",
        ConfigurationOptions.ES_NODES_WAN_ONLY->"true",
        ConfigurationOptions.ES_NODES->url,
        "es.read.field.include" -> "Country, CountryCode,Confirmed,Deaths, Recovered,Date",
        // Option to specify a serialization reader
        ConfigurationOptions.ES_SERIALIZATION_READER_VALUE_CLASS -> classOf[SpecificBasicDateTimeReader].getCanonicalName
      ))
      .groupBy($"Country", $"CountryCode", $"Date").agg(expr("sum(Confirmed) as Confirmed")
      , expr("sum(Deaths) as Deaths")
      , expr("sum(Recovered) as Recovered"))


    println("df.printSchema()")
    df.printSchema()



    val ndf = df.withColumn("nextDate",date_add($"Date",1))
      .withColumn("NConfirmed", $"Confirmed")
      .withColumn("NDeaths", $"Deaths")
      .withColumn("NRecovered", $"Recovered")
      .select("Country", "NConfirmed","NDeaths", "NRecovered", "nextDate")

    val pgroupedDf =  df.join(
      ndf,
      df("Country") <=> ndf("Country")
        && df("Date") <=> ndf("nextDate"),
      "left_outer"
    )
      .cache()

    val groupedDf = pgroupedDf
      .withColumn("Confirmed", $"Confirmed"-$"NConfirmed")
      .withColumn("Deaths", $"Deaths"-$"NDeaths")
      .withColumn("Recovered", $"Recovered"-$"NRecovered") //udf((x:Long)=>if(x != null) x else 0, LongType)(odf("Recovered"))
      .select(df("Country"), df("CountryCode"), $"Date",  $"Confirmed", $"Deaths", $"Recovered")



    val oldXDaysDf = groupedDf.where($"Date">=date_sub(current_date(), 30))

    //Input DataFrame to complete with predictions
    val inputDf = groupedDf.union(oldXDaysDf
      .withColumn("Date",date_add($"Date",30)))
      .withColumn("DateInt", (dayofmonth(df("Date"))/1.3)+(lit(30) * month(df("Date"))))
     .cache()



    val model = CrossValidatorModel.load("models/GBTRegressionModel_total_cov_v2.ml")



    println(s"Best params :\n ${model.bestModel.explainParams()}")




    val indexedDf = new StringIndexer()
      .setInputCol("Country")
      .setOutputCol("CountryIndex")
      .fit(inputDf)
      .transform(inputDf)

    val readyDf = new VectorAssembler().setInputCols(Array("CountryIndex", "DateInt"))
        .setOutputCol("features")
        .transform(indexedDf)

    println("prediction computation...")
    // Make predictions.
    val predictionDf = model.transform(readyDf)

    println("prediction computed...")

    predictionDf.printSchema()



  //val finalDf = inputDf.withColumn("Predicted_Confirmed_Cases", predictionDf.col("prediction"))
  val finalDf = predictionDf.withColumn("Predicted_Confirmed", predictionDf.col("prediction"))
                  .select("Country", "CountryCode", "Date", "Confirmed", "Predicted_Confirmed", "Recovered", "Deaths")


    finalDf.printSchema()
    finalDf.head(5)

    saveDataWithPredictionstoES(finalDf)
  }


  def saveDataWithPredictionstoES(finalDf:DataFrame): Unit ={
    EsSparkSQL.saveToEs(finalDf
      .withColumn("Date",date_format($"Date","dd-MM-yyyy HH:mm:ss"))
      .withColumn("Predicted_Recovered", lit(-1))
      .withColumn("Predicted_Deaths", lit(-1))
      , Map(
      ConfigurationOptions.ES_RESOURCE -> "covid19_prediction_data_spark/_doc",
      ConfigurationOptions.ES_SPARK_DATAFRAME_WRITE_NULL_VALUES -> "true",
      ConfigurationOptions.ES_WRITE_OPERATION -> "create",
      ConfigurationOptions.ES_NET_HTTP_AUTH_USER->"elastic",
      ConfigurationOptions.ES_NET_HTTP_AUTH_PASS->"W|Hed%/E$]=(",
      ConfigurationOptions.ES_NODES_WAN_ONLY->"true",
      ConfigurationOptions.ES_NODES->url
    ))
  }


}