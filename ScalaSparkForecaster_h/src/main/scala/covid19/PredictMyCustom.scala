package covid19

import java.sql.{Date, Timestamp}

import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType
import org.elasticsearch.hadoop.cfg.ConfigurationOptions
import org.elasticsearch.spark.sql.EsSparkSQL


/** Main class */
object PredictMyCustom {

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


  val HORIZON:Int = 7 //en jours


  def addNtoColumnsName(df:DataFrame, columnNames:Vector[String]):DataFrame = {
    if(columnNames.isEmpty) df
    else addNtoColumnsName(df, columnNames.tail).withColumnRenamed(columnNames.head, "N_"+columnNames.head)
  }


  def valeurMesureHorizonh(df: DataFrame, h: Int,   dateColumnName:String, featureColumnName:String, keyColumnsNames:String*): DataFrame = {

    val ndf = addNtoColumnsName(df
      //.withColumn("old_"+dateColumnName,df(dateColumnName))
      .withColumn(dateColumnName,date_add(df(dateColumnName),h)), df.columns.toVector)



    //keyColumnsNames.map(c=>(df.col(c)<=>ndf.col("N_"+c))).reduce(_ && _).explain(true)



    val pgroupedDf =  df.
      join(
        ndf,
        keyColumnsNames.map(c=>df.col(c)<=>ndf.col("N_"+c)).reduce(_ && _),
        "left_outer"
      )



    pgroupedDf
      //.withColumn(featureColumnName+"_"+h, pgroupedDf(featureColumnName)-pgroupedDf("N_"+featureColumnName))
      .withColumn(featureColumnName+"_"+h, pgroupedDf("N_"+featureColumnName))
      //.withColumn(dateColumnName+"_"+h, pgroupedDf("old_"+dateColumnName))
      //.select(featureColumnName+"_"+h, (dateColumnName+"_"+h)+:df.columns: _*)
      .select(featureColumnName+"_"+h, df.columns: _*)


  }


  def joinValeurMesureHorizonh(df1: DataFrame, df2: DataFrame,  featureColumnName:String, keyColumnsNames:String*): DataFrame = {

    /* println("df1.printSchema()")
     df1.printSchema()
     println("df2.printSchema()")
     df2.printSchema()*/

    val ndf2 = addNtoColumnsName(df2, keyColumnsNames.toVector).withColumnRenamed(featureColumnName, "N_"+featureColumnName)


    val pgroupedDf =  df1.
      join(
        ndf2,
        keyColumnsNames.map(c=>df1.col(c)<=>ndf2.col("N_"+c)).reduce(_ && _),
        "inner"
      )

    pgroupedDf.drop("N_"+featureColumnName).drop(keyColumnsNames.map("N_"+_):_*)

  }

  def addTimeValColumns(df:DataFrame,  dateColumnName:String, featureColumnName:String, keyColumnsNames:String*):DataFrame = {
    (1 to HORIZON).par.aggregate(df)((b, h)=>valeurMesureHorizonh(b, h, dateColumnName, featureColumnName, keyColumnsNames:_*), joinValeurMesureHorizonh(_, _, featureColumnName, keyColumnsNames:_*))

    /*if(h==0) df
    else {
       valeurMesureHorizonh(addTimeValColumns(df, h-1, dateColumnName, featureColumnName, keyColumnsNames:_*), h, dateColumnName, featureColumnName, keyColumnsNames:_*)
    }*/
  }

  /** Main function */
  def main(args: Array[String]): Unit = {


    val groupedDf = spark.read.format("csv")
      .option("header","false")
      .option("inferSchema", "true")
      .load("grouped_coviddata.csv")
      .toDF("Country", "CountryCode", "Date", "DateInt",  "Confirmed", "Deaths", "Recovered")
      .withColumn("Confirmed",$"Confirmed".cast(LongType))
      .withColumn("Deaths",$"Deaths".cast(LongType))
      .withColumn("Recovered",$"Recovered".cast(LongType))
      .na.fill(0)



    val oldXDaysDf = groupedDf.where($"Date">=date_sub(current_date(), HORIZON))
      .withColumn("Date",date_add($"Date",HORIZON))
      /*.withColumn("Confirmed", lit(0))
      .withColumn("Deaths", lit(0))
      .withColumn("Recovered", lit(0))*/

    //Input DataFrame to complete with predictions
    val inputDf = groupedDf.union(oldXDaysDf)
     .cache()

    println("inputDf.printSchema()")
    inputDf.printSchema()
    inputDf.show(10)


    val adjustedGroupedDf = addTimeValColumns(inputDf.select("Country",  "Date",  "Confirmed")
      , "Date", "Confirmed", "Country", "Date")
      .na.fill(0)
      .cache()

    /*println("adjustedGroupedDf.printSchema()")
    adjustedGroupedDf.printSchema()
    adjustedGroupedDf.show(10)*/



    val indexedDf = new StringIndexer()
      .setInputCol("Country")
      .setOutputCol("CountryIndex")
      .fit(adjustedGroupedDf)
      .transform(adjustedGroupedDf)
      .withColumn("Confirmed", udf((t:Timestamp, r:Date, c:Long)=>if(t.compareTo(r)>=0) 0 else c, LongType)($"Date", current_date(), $"Confirmed"))


    val featureColums = "CountryIndex"+:indexedDf.columns.filter(_.startsWith("Confirmed_"))


    val readyDf = //new VectorAssembler().setInputCols(Array("CountryIndex", "Date", "Recovered_Cases","Dead_Cases"))
      new VectorAssembler().setInputCols(featureColums)
        .setOutputCol("features")
        .transform(indexedDf)
        //.withColumn("label", $"Confirmed")
        .select("features", "Country",  "Date",  "Confirmed").cache()



    val model = CrossValidatorModel.load("models/GBTRegressionModel_total_cov_v3_short.ml")



    //println(s"Best params :\n ${model.bestModel.explainParams()}")





    println("prediction computation...")
    // Make predictions.
    val predictionDf = model.transform(readyDf)

    println("prediction computed...")

    predictionDf.printSchema()



  //val finalDf = inputDf.withColumn("Predicted_Confirmed_Cases", predictionDf.col("prediction"))
  val finalDf = predictionDf.withColumn("Predicted_Confirmed", predictionDf.col("prediction"))
                  .select("Country",  "Date", "Confirmed", "Predicted_Confirmed")


    finalDf.printSchema()
    finalDf.head(5)

    saveDataWithPredictionstoES(finalDf)
  }


  def saveDataWithPredictionstoES(finalDf:DataFrame): Unit ={
    EsSparkSQL.saveToEs(finalDf
      .withColumn("Date",date_format($"Date","dd-MM-yyyy HH:mm:ss"))
      , Map(
      ConfigurationOptions.ES_RESOURCE -> "covid19_prediction_data_spark_mycustom/_doc",
      ConfigurationOptions.ES_SPARK_DATAFRAME_WRITE_NULL_VALUES -> "true",
      ConfigurationOptions.ES_WRITE_OPERATION -> "create",
      ConfigurationOptions.ES_NET_HTTP_AUTH_USER->"elastic",
      ConfigurationOptions.ES_NET_HTTP_AUTH_PASS->"W|Hed%/E$]=(",
      ConfigurationOptions.ES_NODES_WAN_ONLY->"true",
      ConfigurationOptions.ES_NODES->url
    ))
  }


}