package covid19

import java.sql.{Date, Timestamp}

import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, LongType, StructField, StructType}
import org.apache.spark.storage.StorageLevel
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
      .config("spark.executor.memory", "4g")
      .config("spark.driver.memory", "4g")
      .config("spark.cleaner.referenceTracking.cleanCheckpoints", "true")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  spark.sparkContext.setCheckpointDir("tmp/checkpoint")

  // For implicit conversions like converting RDDs to DataFrames
  import spark.implicits._

  val url = "ec2-3-135-203-125.us-east-2.compute.amazonaws.com:9200,ec2-13-58-53-1.us-east-2.compute.amazonaws.com:9200,ec2-18-220-232-235.us-east-2.compute.amazonaws.com:9200,ec2-18-224-63-68.us-east-2.compute.amazonaws.com:9200"


  val HORIZON:Int = 20 //en jours
  val PREDICTION_HORIZON:Int = 7 //en jours


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
      ).na.fill(0.0)



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

    val ndf2 = addNtoColumnsName(df2, df2.columns.filterNot(_.startsWith("Confirmed_")).toVector)


    val pgroupedDf =  df1.
      join(
        ndf2,
        keyColumnsNames.map(c=>df1.col(c)<=>ndf2.col("N_"+c)).reduce(_ && _),
        "inner"
      )

    pgroupedDf.drop(pgroupedDf.columns.filter(_.startsWith("N_")):_*)
  }

  def addTimeValColumns(df:DataFrame,  dateColumnName:String, featureColumnName:String, keyColumnsNames:String*):DataFrame = {
    (1 to HORIZON).par.aggregate(df)((b, h)=>valeurMesureHorizonh(b, h, dateColumnName, featureColumnName, keyColumnsNames:_*), joinValeurMesureHorizonh(_, _, featureColumnName, keyColumnsNames:_*))

    /*if(h==0) df
    else {
       valeurMesureHorizonh(addTimeValColumns(df, h-1, dateColumnName, featureColumnName, keyColumnsNames:_*), h, dateColumnName, featureColumnName, keyColumnsNames:_*)
    }*/
  }


  def computeValeurMesurehDataSpecific(h:Int,  currentDate: Date, currentDf:DataFrame, df:DataFrame,  dateColumnName:String, featureColumnName:String, keyColumnsNames:String*): DataFrame = {


    val ndf0 = df.where(col(dateColumnName)<=>date_sub(lit(currentDate), h))

    //println(s"Found data for h=$h at date= ${ndf0.first().getTimestamp(ndf0.first().fieldIndex("Date"))}")

    if(ndf0 !=null) {
      if(!currentDf.columns.exists(_.startsWith("Confirmed_"))){
        ndf0
          .withColumnRenamed(featureColumnName, featureColumnName+"_"+h)
          .withColumn(dateColumnName, lit(currentDate))
          .select(featureColumnName+"_"+h, currentDf.columns:_*)
      }
      else {
        val pgroupedDf =  currentDf
          .join(
            ndf0.select(featureColumnName, keyColumnsNames:_*)
              .withColumn(dateColumnName, lit(currentDate)),
            keyColumnsNames,
            "left_outer"
          ).na.fill(0.0)

        pgroupedDf
          .withColumnRenamed(featureColumnName, featureColumnName+"_"+h)
          .select(featureColumnName+"_"+h, currentDf.columns:_*)
      }

    } else
      currentDf
        .withColumn(featureColumnName+"_"+h, lit(0))


  }


  /*
  @params: previousDateDf: is a dataFrame with the right schema and countries for the last known Day

  df doit contenir Country et CountryIndex
   */
  def predictDatebyDate(h:Int, previousDateDf:DataFrame, df:DataFrame, model:CrossValidatorModel, dateColumnName:String, featureColumnName:String, keyColumnsNames:String*):DataFrame = {
    if(h==0) df
    else{

      val newDf = predictDatebyDate(h-1, previousDateDf, df, model,  dateColumnName, featureColumnName, keyColumnsNames:_*)

      val currentDf = previousDateDf.withColumn(dateColumnName, date_add(previousDateDf.col(dateColumnName),h))
      val currentDate = currentDf.first().getDate(currentDf.first().fieldIndex("Date"))
      println(s"Handling date $currentDate")

      val currentDfEnriched = (1 to HORIZON).par.aggregate(currentDf)((b, i)=>computeValeurMesurehDataSpecific(i, currentDate, b, newDf, dateColumnName, featureColumnName, keyColumnsNames:_*), joinValeurMesureHorizonh(_, _, featureColumnName, keyColumnsNames:_*))


      val readyDf = //new VectorAssembler().setInputCols(Array("CountryIndex", "Date", "Recovered_Cases","Dead_Cases"))
        new VectorAssembler().setInputCols("CountryIndex"+:currentDfEnriched.columns.filter(_.startsWith("Confirmed_")))
          .setOutputCol("features")
          .transform(currentDfEnriched)
          .select("features", currentDf.columns:_*)



      println(s"prediction computation for  ${currentDate}...")

      val finalDf = model.transform(readyDf)
        .withColumn("prediction",udf((x:Double)=>x.round.toInt, IntegerType)($"prediction"))
        .withColumn("Confirmed", $"prediction")
        .withColumnRenamed("prediction", "Predicted_Confirmed")
        .drop("features")

      println(s"prediction computed  for  ${currentDate} ...")


      newDf.union(finalDf).checkpoint()//.cache()



    }
  }



  /** Main function */
  def main(args: Array[String]): Unit = {


    val groupedDf = spark.read.format("csv")
      .option("header","false")
      .option("inferSchema", "true")
      .load("grouped_coviddata.csv")
      .toDF("Country", "CountryCode", "Date", "DateInt",  "Confirmed", "Deaths", "Recovered")
      .withColumn("Confirmed",$"Confirmed".cast(IntegerType))
      .withColumn("Deaths",$"Deaths".cast(IntegerType))
      .withColumn("Recovered",$"Recovered".cast(IntegerType))



    val indexedDf = new StringIndexer()
      .setInputCol("Country")
      .setOutputCol("CountryIndex")
      .fit(groupedDf)
      .transform(groupedDf)
      .withColumn("CountryIndex",udf((x:Double)=>x.toInt, IntegerType)($"CountryIndex"))
      .select("Country", "CountryIndex", "Date",  "Confirmed")

    val countryDf = indexedDf.select("Country", "CountryIndex").distinct()


    val adjustedGroupedDf = addTimeValColumns(indexedDf.drop("Country"), "Date", "Confirmed", "CountryIndex", "Date")



    val model = CrossValidatorModel.load("models/GBTRegressionModel_total_cov_v3.ml")


    val featureColums = "CountryIndex"+:adjustedGroupedDf.columns.filter(_.startsWith("Confirmed_"))


    val readyDf = //new VectorAssembler().setInputCols(Array("CountryIndex", "Date", "Recovered_Cases","Dead_Cases"))
      new VectorAssembler().setInputCols(featureColums)
        .setOutputCol("features")
        .transform(adjustedGroupedDf)
        .select("features",  "CountryIndex",  "Date",  "Confirmed")



    println("prediction computation for initial df ...")
    // Make predictions.
    val resultDf = model.transform(readyDf).withColumn("prediction",udf((x:Double)=>x.round.toInt, IntegerType)($"prediction")).withColumnRenamed("prediction", "Predicted_Confirmed")
      .drop("features").cache()

    println("prediction computed for initial df ...")


    val maxKnownDateRow = groupedDf.select($"Date").orderBy($"Date".desc).first()
    val maxKnownDate = new Date(maxKnownDateRow.getTimestamp(maxKnownDateRow.fieldIndex("Date")).getTime)

    println(s"last known date is $maxKnownDate")


    val fDf = predictDatebyDate(PREDICTION_HORIZON, resultDf.where($"Date"<=>lit(maxKnownDate)).drop("Predicted_Confirmed", "Confirmed").cache(), resultDf, model,  "Date", "Confirmed",  "CountryIndex", "Date")
        .withColumn("Confirmed", udf((t:Timestamp, r:Date, c:Integer)=>if(t.compareTo(r)>0) 0 else c, IntegerType)($"Date", lit(maxKnownDate), $"Confirmed"))


    val finalDf = fDf.
      join(
        countryDf,
        fDf("CountryIndex")<=>countryDf("CountryIndex"),
        "left_outer"
      ).select(countryDf("Country"), fDf("Date"),  fDf("Confirmed"), fDf("Predicted_Confirmed"))

    println("finalDf.printSchema()")
    finalDf.printSchema()


    //finalDf.write.format("csv").save("finalDfcsv")

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