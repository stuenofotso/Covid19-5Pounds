package covid19.kaggle

import java.sql.Date

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode}


/** Main class */
object PredictMyCustomKaggleData {

  import org.apache.spark.sql.SparkSession

  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Covid19 Forecast")
      .config("spark.master", "local[16]")
      .config("spark.executor.memory", "8096M")
      .config("spark.driver.memory", "8096M")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")


  // For implicit conversions like converting RDDs to DataFrames
  import spark.implicits._

  spark.sparkContext.setCheckpointDir("tmp/checkpoints")



  val HORIZON:Int = 40 //en jours
  val PREDICTION_HORIZON:Int = 30 //en jours



  def addNtoColumnsName(df:DataFrame, columnNames:Vector[String]):DataFrame = {
    if(columnNames.isEmpty) df
    else addNtoColumnsName(df, columnNames.tail).withColumnRenamed(columnNames.head, "N_"+columnNames.head)
  }


  def valeurMesureHorizonh(df: DataFrame, h: Int,   DateColumnName:String, featureColumnName:String, keyColumnsNames:String*): DataFrame = {

    val ndf = addNtoColumnsName(df
      //.withColumn("old_"+DateColumnName,df(DateColumnName))
      .withColumn(DateColumnName,date_add(df(DateColumnName),h)), df.columns.toVector)



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
      //.withColumn(DateColumnName+"_"+h, pgroupedDf("old_"+DateColumnName))
      //.select(featureColumnName+"_"+h, (DateColumnName+"_"+h)+:df.columns: _*)
      .select(featureColumnName+"_"+h, df.columns: _*)


  }


  def joinValeurMesureHorizonh(df1: DataFrame, df2: DataFrame,  featureColumnName:String, keyColumnsNames:String*): DataFrame = {


    val ndf2 = addNtoColumnsName(df2, df2.columns.filterNot(_.startsWith(featureColumnName+"_")).toVector)


    val pgroupedDf =  df1.
      join(
        ndf2,
        keyColumnsNames.map(c=>df1.col(c)<=>ndf2.col("N_"+c)).reduce(_ && _),
        "inner"
      )

    pgroupedDf.drop(pgroupedDf.columns.filter(_.startsWith("N_")):_*)
  }

  def addTimeValColumns(df:DataFrame,  DateColumnName:String, featureColumnName:String, keyColumnsNames:String*):DataFrame = {
    (1 to HORIZON).par.aggregate(df)((b, h)=>valeurMesureHorizonh(b, h, DateColumnName, featureColumnName, keyColumnsNames:_*), joinValeurMesureHorizonh(_, _, featureColumnName, keyColumnsNames:_*))

    /*if(h==0) df
    else {
       valeurMesureHorizonh(addTimeValColumns(df, h-1, DateColumnName, featureColumnName, keyColumnsNames:_*), h, DateColumnName, featureColumnName, keyColumnsNames:_*)
    }*/
  }


  def computeValeurMesurehDataSpecific(h:Int,  currentDate: Date, currentDf:DataFrame, df:DataFrame,  DateColumnName:String, featureColumnName:String, keyColumnsNames:String*): DataFrame = {


    val ndf0 = df.where(col(DateColumnName)<=>date_sub(lit(currentDate), h))

    //println(s"Found data for h=$h at Date= ${ndf0.first().getTimestamp(ndf0.first().fieldIndex("Date"))}")

    if(ndf0 !=null) {
      if(!currentDf.columns.exists(_.startsWith(featureColumnName+"_"))){
        ndf0
          .withColumnRenamed(featureColumnName, featureColumnName+"_"+h)
          .withColumn(DateColumnName, lit(currentDate))
          .select(featureColumnName+"_"+h, currentDf.columns:_*)
      }
      else {
        val pgroupedDf =  currentDf
          .join(
            ndf0.select(featureColumnName, keyColumnsNames:_*)
              .withColumn(DateColumnName, lit(currentDate)),
            keyColumnsNames,
            "left_outer"
          ).na.fill(0.0)

        pgroupedDf
          .withColumnRenamed(featureColumnName, featureColumnName+"_"+h)
          .select(featureColumnName+"_"+h, currentDf.columns:_*)
      }

    } else
      currentDf
        .withColumn(featureColumnName+"_"+h, lit(0.0))


  }



  /*
  @params: previousDateDf: is a dataFrame with the right schema and countries for the last known Day

  df doit contenir Country et CountryIndex
   */
  @scala.annotation.tailrec
  def predictDatebyDate(h:Int, H:Int,  previousDateDf:DataFrame, df:DataFrame, model:CrossValidatorModel, DateColumnName:String, featureColumnName:String, keyColumnsNames:String*):DataFrame = {
    if(h==H+1) df
    else{

      val currentDf = previousDateDf.withColumn(DateColumnName, date_add(previousDateDf.col(DateColumnName),h))
      val currentDate = currentDf.first().getAs[Date](DateColumnName)
      println(s"Handling Date $currentDate for feature $featureColumnName")

      val currentDfEnriched = (1 to HORIZON).par.aggregate(currentDf)((b, i)=>computeValeurMesurehDataSpecific(i, currentDate, b, df, DateColumnName, featureColumnName, keyColumnsNames:_*), joinValeurMesureHorizonh(_, _, featureColumnName, keyColumnsNames:_*))



      val readyDf = //new VectorAssembler().setInputCols(Array("CountryIndex", "Date", "Recovered_Cases","Dead_Cases"))
        new VectorAssembler().setInputCols("Country_RegionIndex"+:"Province_StateIndex"+:currentDfEnriched.columns.filter(_.startsWith(featureColumnName+"_")))
          .setOutputCol("features")
          .transform(currentDfEnriched)
          .select("features", currentDf.columns:_*)



      println(s"prediction computation for  ${currentDate}, feature $featureColumnName...")

      val finalDf = model.transform(readyDf)
        .withColumnRenamed("prediction", featureColumnName)
        .drop("features")
          //.cache()
      //.localCheckpoint(false)
          .checkpoint()

      println(s"prediction computed  for  ${currentDate}, feature $featureColumnName...")


      predictDatebyDate(h+1, H,  previousDateDf, df.union(finalDf), model,  DateColumnName, featureColumnName, keyColumnsNames:_*)


    }
  }

  def predictForFeatureAndTimeWindow(indexedDf:DataFrame, featureName:String, timeWindow:Int, keyColumnsNames:String*): DataFrame={


    val model = CrossValidatorModel.load(s"models/KaggleDataModel_v2/GBTRegressionModel_total_cov_$featureName.ml")



    val maxKnownDateRow = indexedDf.select($"Date").orderBy($"Date".desc).first()
    val maxKnownDate = new Date(maxKnownDateRow.getTimestamp(maxKnownDateRow.fieldIndex("Date")).getTime)

    println(s"last known Date is $maxKnownDate")

    val filteredDf = indexedDf.where($"Date">=date_sub(lit(maxKnownDate), Math.max(HORIZON, PREDICTION_HORIZON))).cache()



    predictDatebyDate(1, timeWindow, filteredDf.where($"Date"<=>lit(maxKnownDate)).drop( featureName).cache(), filteredDf, model,  "Date", featureName,  keyColumnsNames:_*)
      .withColumn(featureName, sum(featureName).over(
        Window
          .partitionBy("Country_RegionIndex", "Province_StateIndex")
          .orderBy($"Date".asc)))

  }




  /** Main function */
  def main(args: Array[String]): Unit = {


    val inputDf =  spark.read.format("csv")
      .option("header","false")
      .option("inferSchema", "true")
      .load(s"kaggle_data/in/fullInputDf")
      .toDF("Country_RegionIndex", "Province_StateIndex", "Date", "ConfirmedCases", "Fatalities")



    inputDf.printSchema()


    val resDf = Seq("ConfirmedCases", "Fatalities").par
      .map(feature=>predictForFeatureAndTimeWindow(inputDf.select("Country_RegionIndex", "Province_StateIndex", "Date", feature), feature, PREDICTION_HORIZON, "Country_RegionIndex", "Province_StateIndex", "Date").cache())



    val predictedDf = resDf.head.join(resDf.last, Seq("Country_RegionIndex", "Province_StateIndex", "Date"))
      .select(resDf.head("Country_RegionIndex"), resDf.head("Province_StateIndex"), resDf.head("Date"), col("ConfirmedCases"), col("Fatalities"))


    val countryProvinceDf = spark.read.format("csv")
      .option("header","false")
      .option("inferSchema", "true")
      .load("kaggle_data/internal/countryIndexCSV")
      .toDF("Country_Region", "Country_RegionIndex", "Province_State", "Province_StateIndex")


    val finalDf = predictedDf.
      join(
        countryProvinceDf,
        Seq("Country_RegionIndex", "Province_StateIndex"),
        "left_outer"
      ).select(countryProvinceDf("Country_Region"), countryProvinceDf("Province_State"), predictedDf("Date"),  predictedDf("ConfirmedCases"), predictedDf("Fatalities"))

    println("finalDf.printSchema()")
    finalDf.printSchema()
    finalDf.show(5)

    //predictedDf.write.mode(SaveMode.Overwrite).format("csv").save("kaggle_data/tmp/predictedDf")


    val testDf = spark.read.format("csv")
      .option("header","true")
      .option("inferSchema", "true")
      .load("kaggle_data/test.csv")
      .na.fill("RAS", Seq("Province_State"))

    val submissionDf =  testDf.
      join(
        finalDf,
        Seq("Country_Region", "Province_State", "Date"),
        "left_outer"
      ).select(testDf("ForecastId"),  finalDf("ConfirmedCases"), finalDf("Fatalities"))

    println("submissionDf.printSchema()")
    submissionDf.printSchema()

    submissionDf.write.mode(SaveMode.Overwrite).format("csv").save("kaggle_data/tmp/submission")

    merge("kaggle_data/tmp/submission", "kaggle_data/out/submission")

  }


  def merge(srcPath: String, dstPath: String): Unit =  {
    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    FileUtil.copyMerge(hdfs, new Path(srcPath), hdfs, new Path(dstPath), false, hadoopConfig, null)
  }


}