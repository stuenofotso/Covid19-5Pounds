/*
 * Copyright (c) 2020. Steve Tueno; all rights reserved !
 */

package covid19.kaggle

import java.sql.{Date, Timestamp}
import java.time.temporal.ChronoUnit

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.spark.sql.functions.{when, _}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, SaveMode}


/** Main class */
object PredictMyCustomKaggleDataInit {

  import org.apache.spark.sql.SparkSession

  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Covid19 Forecast")
            .config("spark.executor.memory", "8096M")
            .config("spark.driver.memory", "8096M")
      .config("spark.master", "local[16]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")


  // For implicit conversions like converting RDDs to DataFrames
  import spark.implicits._

  //spark.sparkContext.setCheckpointDir("tmp/checkpoints")



  val HORIZON:Int = 40 //en jours



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


  def computeValeurMesurehDataSpecific(h:Int,  currentDate: Date, currentDf:DataFrame, df:DataFrame, accumulator:Map[Date, DataFrame],  DateColumnName:String, featureColumnName:String, keyColumnsNames:String*): DataFrame = {

    val previousDate = currentDf.limit(1).withColumn(DateColumnName, date_sub(col(DateColumnName),h)).first().getAs[Date](DateColumnName)

    val ndf0 = if(accumulator.contains(previousDate)) accumulator(previousDate) else df.where(col(DateColumnName)<=>lit(previousDate))

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
  def predictDatebyDate(h:Int, H:Int, accumulator:Map[Date, DataFrame], previousDateDf:DataFrame, df:DataFrame, model:CrossValidatorModel, DateColumnName:String, featureColumnName:String, keyColumnsNames:String*):Map[Date, DataFrame] = {
    if(h==H+1) accumulator
    else{

      val currentDf = previousDateDf.withColumn(DateColumnName, date_add(previousDateDf.col(DateColumnName),h))
      val currentDate = currentDf.first().getAs[Date](DateColumnName)
      println(s"Handling Date $currentDate for feature $featureColumnName")

      val currentDfEnriched = (1 to HORIZON).par.aggregate(currentDf)((b, i)=>computeValeurMesurehDataSpecific(i, currentDate, b, df, accumulator, DateColumnName, featureColumnName, keyColumnsNames:_*), joinValeurMesureHorizonh(_, _, featureColumnName, keyColumnsNames:_*))



      val readyDf = //new VectorAssembler().setInputCols(Array("CountryIndex", "Date", "Recovered_Cases","Dead_Cases"))
        new VectorAssembler().setInputCols("Country_RegionIndex"+:"Province_StateIndex"+:currentDfEnriched.columns.filter(_.startsWith(featureColumnName+"_")))
          .setOutputCol("features")
          .transform(currentDfEnriched)
          .select("features", currentDf.columns:_*)



      println(s"prediction computation for  ${currentDate}, feature $featureColumnName...")

      val finalDf = model.transform(readyDf)
        .withColumnRenamed("prediction", featureColumnName)
        .drop("features")

      println(s"prediction computed  for  ${currentDate}, feature $featureColumnName...")


      predictDatebyDate(h+1, H, accumulator+(currentDate->finalDf), previousDateDf, df, model,  DateColumnName, featureColumnName, keyColumnsNames:_*)


    }
  }

  def predictForFeatureAndTimeWindow(indexedDf:DataFrame, featureName:String, timeWindow:Int, keyColumnsNames:String*): DataFrame={

    //val adjustedGroupedDf = addTimeValColumns(indexedDf.drop("Country_Region", "Province_State"), "Date", featureName, "Country_RegionIndex", "Province_StateIndex", "Date")

    val model = CrossValidatorModel.load(s"models/KaggleDataModel_v2/GBTRegressionModel_total_cov_$featureName.ml")



    val maxKnownDateRow = indexedDf.select($"Date").orderBy($"Date".desc).first()
    val maxKnownDate = new Date(maxKnownDateRow.getTimestamp(maxKnownDateRow.fieldIndex("Date")).getTime)

    println(s"last known Date is $maxKnownDate")

    val filteredDf = indexedDf.where($"Date">=date_sub(lit(maxKnownDate), HORIZON))



    val predictions = predictDatebyDate(1, timeWindow, Map[Date, DataFrame](), filteredDf.where($"Date"<=>lit(maxKnownDate)).drop( featureName).cache(), filteredDf, model,  "Date", featureName,  keyColumnsNames:_*)

    predictions.values.reduce(_ unionByName  _)
  }




  /** Main function */
  def main(args: Array[String]): Unit = {


    val df = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("kaggle_data/train.csv")
      .drop("Id")
      .withColumn("ConfirmedCases", $"ConfirmedCases".cast(DoubleType))
      .withColumn("Fatalities", $"Fatalities".cast(DoubleType))
      .na.fill(0.0, Seq("ConfirmedCases", "Fatalities"))
      .na.fill("RAS", Seq("Province_State"))


    val ndf = addNtoColumnsName(df.withColumn("Date", date_add($"Date", 1)), df.columns.toVector)

    val pgroupedDf = df.join(
      ndf,
      df("Country_Region") <=> ndf("N_Country_Region")
        && df("Province_State") <=> ndf("N_Province_State")
        && df("Date") <=> ndf("N_Date"),
      "left_outer"
    )


    val groupedDfKaggle = pgroupedDf
      .withColumn("ConfirmedCases", $"ConfirmedCases" - $"N_ConfirmedCases")
      .withColumn("Fatalities", $"Fatalities" - $"N_Fatalities")
      .na.fill(0.0, Seq("ConfirmedCases", "Fatalities"))
      .select(df("Country_Region"), df("Province_State"), df("Date"), col("ConfirmedCases"), col("Fatalities"))

    val maxKnownDateKaggle = new Date(groupedDfKaggle.select($"Date").orderBy($"Date".desc).first().getAs[Timestamp]("Date").getTime)


    val groupedDfES = spark.read.format("csv")
      .option("header", "false")
      .option("inferSchema", "true")
      .load("data/grouped_coviddata.csv")
      .toDF("Country_Region", "Province_State", "Date", "DateInt", "ConfirmedCases", "Fatalities", "Recovered")
      .where($"Date" > lit(maxKnownDateKaggle))
      .select("Country_Region", "Province_State", "Date", "ConfirmedCases", "Fatalities")
      .withColumn("ConfirmedCases", $"ConfirmedCases".cast(DoubleType))
      .withColumn("Fatalities", $"Fatalities".cast(DoubleType))
      .withColumn("Country_Region",
        when(col("Country_Region").equalTo("United States of America"), "US")
          .when(col("Country_Region").equalTo("Côte d'Ivoire"), "Cote d'Ivoire")
          .when(col("Country_Region").equalTo("Cape Verde"), "Cabo Verde")
          .when(col("Country_Region").equalTo("Lao PDR"), "Laos")
          .when(col("Country_Region").equalTo("Russian Federation"), "Russia")
          .when(col("Country_Region").equalTo("Holy See (Vatican City State)"), "Holy See")
          .when(col("Country_Region").equalTo("Iran, Islamic Republic of"), "Iran")
          .when(col("Country_Region").equalTo("Brunei Darussalam"), "Brunei")
          .when(col("Country_Region").equalTo("Saint Vincent and Grenadines"), "Saint Vincent and the Grenadines")
          .when(col("Country_Region").equalTo("Republic of Kosovo"), "Kosovo")
          .when(col("Country_Region").equalTo("Myanmar"), "Burma")
          .when(col("Country_Region").equalTo("Czech Republic"), "Czechia")
          .when(col("Country_Region").equalTo("Tanzania, United Republic of"), "Tanzania")
          .when(col("Country_Region").equalTo("Viet Nam"), "Vietnam")
          .when(col("Country_Region").equalTo("Venezuela (Bolivarian Republic)"), "Venezuela")
          .when(col("Country_Region").equalTo("Korea (South)"), "Korea, South")
          .when(col("Country_Region").equalTo("Macedonia, Republic of"), "North Macedonia")
          .when(col("Country_Region").equalTo("Swaziland"), "Eswatini")
          .when(col("Country_Region").equalTo("Syrian Arab Republic (Syria)"), "Syria")
          .when(col("Country_Region").equalTo("Taiwan, Republic of China"), "Taiwan*")
          .otherwise(col("Country_Region"))
      )



    val testDf = spark.read.format("csv")
      .option("header","true")
      .option("inferSchema", "true")
      .load("kaggle_data/test.csv")
      .na.fill("RAS", Seq("Province_State"))

    val filteredGroupedDfES =  groupedDfES.
      join(
        testDf,
        Seq("Country_Region", "Province_State", "Date")
      ).select(groupedDfES("Country_Region"), groupedDfES("Province_State"), groupedDfES("Date"), groupedDfES("ConfirmedCases"), groupedDfES("Fatalities"))
        .cache()


    //println(s"groupeddfES after count=${filteredGroupedDfES.count()}")

    filteredGroupedDfES.printSchema()
/*

    val minKnownDateES = new Date(groupedDfES.select($"Date").orderBy($"Date").first().getAs[Timestamp]("Date").getTime)

    println(s"min known date  ES=$minKnownDateES")
*/



    val maxKnownDateES = new Date(filteredGroupedDfES.select($"Date").orderBy($"Date".desc).first().getAs[Timestamp]("Date").getTime)

    println(s"known dates : Kaggle=$maxKnownDateKaggle ; ES=$maxKnownDateES")


    val prevGroupedDf = groupedDfKaggle
      .select("Country_Region", "Province_State", "Date", "ConfirmedCases", "Fatalities")
      .unionByName(
        filteredGroupedDfES.select("Country_Region", "Province_State", "Date", "ConfirmedCases", "Fatalities")
      )




    /*val prevIndexedDfCountry = new StringIndexer()
      .setInputCol("Country_Region")
      .setOutputCol("Country_RegionIndex")
      .fit(prevGroupedDf)
      .transform(prevGroupedDf)

    val prevIndexedDf = new StringIndexer()
      .setInputCol("Province_State")
      .setOutputCol("Province_StateIndex")
      .fit(prevIndexedDfCountry)
      .transform(prevIndexedDfCountry).cache()




    val countryProvinceDf = prevIndexedDf.select("Country_Region", "Country_RegionIndex", "Province_State", "Province_StateIndex").distinct()

    countryProvinceDf.write.mode(SaveMode.Overwrite).format("csv").save("kaggle_data/internal/countryIndexCSV")
*/


    val countryProvinceDf = spark.read.format("csv")
      .option("header", "false")
      .option("inferSchema", "true")
      .load("kaggle_data/internal/countryIndexCSV")
      .toDF("Country_Region", "Country_RegionIndex", "Province_State", "Province_StateIndex")


    val prevIndexedDf = prevGroupedDf.join(countryProvinceDf, Seq("Country_Region", "Province_State"))
      //.drop(countryProvinceDf("Country_Region")).drop(countryProvinceDf("Province_State"))


    prevIndexedDf.printSchema()


    val unfoundCountryKaggleDf = prevIndexedDf.filter($"Country_Region".isin("MS Zaandam", "West Bank and Gaza", "Diamond Princess")).cache()




    val resUnfoundCountryKaggleDf = Seq("ConfirmedCases", "Fatalities").par.map(feature => predictForFeatureAndTimeWindow(unfoundCountryKaggleDf.select("Country_RegionIndex",  "Province_StateIndex", "Date", feature), feature, maxKnownDateKaggle.toLocalDate.until(maxKnownDateES.toLocalDate, ChronoUnit.DAYS).toInt, "Country_RegionIndex", "Province_StateIndex", "Date"))


    val predictedUnfoundCountryKaggleDf = resUnfoundCountryKaggleDf.head.join(resUnfoundCountryKaggleDf.last, Seq("Country_RegionIndex", "Province_StateIndex", "Date"))
      .select(resUnfoundCountryKaggleDf.head("Country_RegionIndex"), resUnfoundCountryKaggleDf.head("Province_StateIndex"), resUnfoundCountryKaggleDf.head("Date"), col("ConfirmedCases"), col("Fatalities"))
        .cache()


    predictedUnfoundCountryKaggleDf.printSchema()

    val inputDf = prevIndexedDf.select("Country_RegionIndex", "Province_StateIndex", "Date", "ConfirmedCases", "Fatalities")
      .unionByName(predictedUnfoundCountryKaggleDf.select("Country_RegionIndex", "Province_StateIndex", "Date", "ConfirmedCases", "Fatalities"))
        .distinct()

      /*.checkpoint()


    val resDf = Seq("ConfirmedCases", "Fatalities").par
      .map(feature=>predictForFeatureAndTimeWindow(inputDf.select("Country_RegionIndex", "Province_StateIndex", "Date", feature), feature, 3, "Country_RegionIndex", "Province_StateIndex", "Date"))



    val predictedf = resDf.head.join(resDf.last, Seq("Country_RegionIndex", "Province_StateIndex", "Date"))
      .select(resDf.head("Country_RegionIndex"), resDf.head("Province_StateIndex"), resDf.head("Date"), col("ConfirmedCases"), col("Fatalities"))

    val res = inputDf.union(predictedf).checkpoint()

*/

    inputDf.write.mode(SaveMode.Overwrite).format("csv").save("kaggle_data/in/fullInputDf")

    merge("kaggle_data/in/fullInputDf", "kaggle_data/in/fullInputDf.csv")
  }

  def merge(srcPath: String, dstPath: String): Unit =  {
    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    FileUtil.copyMerge(hdfs, new Path(srcPath), hdfs, new Path(dstPath), false, hadoopConfig, null)
  }

}