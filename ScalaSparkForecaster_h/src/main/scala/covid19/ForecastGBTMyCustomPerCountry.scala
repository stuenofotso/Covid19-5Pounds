package covid19

import java.io.File

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.GBTRegressor
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{date_add, udf}
import org.apache.spark.sql.types.{IntegerType, LongType}


/** Main class */
object ForecastGBTMyCustomPerCountry {

  import org.apache.spark.sql.SparkSession

  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Covid19 Forecast")
      .config("spark.master", "local[16]")
      .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  // For implicit conversions like converting RDDs to DataFrames
  import spark.implicits._

val HORIZON:Int = 15 //en jours


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
      .withColumn("Confirmed",$"Confirmed".cast(IntegerType))
      .withColumn("Deaths",$"Deaths".cast(IntegerType))
      .withColumn("Recovered",$"Recovered".cast(IntegerType))


    val countryDf = spark.read.format("csv")
      .option("header","false")
      .option("inferSchema", "true")
      .load("countryIndexCSV")
      .toDF("Country", "N_CountryIndex")




    val indexedDf = groupedDf.join(countryDf, "Country")
      .select("N_CountryIndex", groupedDf.columns:_*)
        .withColumnRenamed("N_CountryIndex", "CountryIndex")
      .select("Country", "CountryIndex", "Date",  "Confirmed").cache()


    indexedDf.printSchema()


      /*new StringIndexer()
      .setInputCol("Country")
      .setOutputCol("CountryIndex")
      .fit(groupedDf)
      .transform(groupedDf)
      .withColumn("CountryIndex",udf((x:Double)=>x.toInt, IntegerType)($"CountryIndex"))
      .select("Country", "CountryIndex", "Date",  "Confirmed").cache()

    val countryDf2 = indexedDf.select("Country", "CountryIndex").distinct()

    countryDf.write.format("csv").mode("overwrite").save("countryIndexCSV")
*/

    countryDf
      .withColumnRenamed("N_CountryIndex", "CountryIndex")
      .collect().par.foreach(r=>learnModelPerCountry(r, indexedDf.drop("Country")))

    //learnModelPerCountry(countryDf.withColumnRenamed("N_CountryIndex", "CountryIndex").first(), indexedDf.drop("Country"))

  }

  def learnModelPerCountry(countryRow:Row, indexedDf:DataFrame){

    if(new File(s"models/perCountryModels/GBTRegressionModel_Country_${countryRow.getAs[Int]("CountryIndex")}.ml").exists()) return

    val adjustedGroupedDf = addTimeValColumns(indexedDf.where($"CountryIndex"<=>countryRow.getAs[Integer]("CountryIndex")), "Date", "Confirmed", "CountryIndex", "Date")
      .na.fill(0)
      .cache()


    val readyDf = //new VectorAssembler().setInputCols(Array("CountryIndex", "Date", "Recovered_Cases","Dead_Cases"))
      new VectorAssembler().setInputCols(adjustedGroupedDf.columns.filter(_.startsWith("Confirmed_")))
        .setOutputCol("features")
        .transform(adjustedGroupedDf)
        .withColumn("label", $"Confirmed")
        .select("features", "label")


    // Split the data into train and test
    val splits = readyDf.randomSplit(Array(0.9, 0.1), seed = 1234L)
    val trainData = splits(0)
    val testData = splits(1)





    // Train a GBT model.
    val gbt = new GBTRegressor()
      .setLabelCol("label")
      .setFeaturesCol("features")
      //.setMaxIter(10)


    val  paramGrid = new ParamGridBuilder()
      //.addGrid(gbt.maxBins, Array(190))
      .addGrid(gbt.maxDepth, Array(10, 20))
      .build()


    val cv = new CrossValidator()
      .setEstimator(gbt)
      .setEvaluator(new RegressionEvaluator())
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(3)
      .setParallelism(3)

    println(s"begin of model training for country ${countryRow.getAs[String]("Country")}...")
    val model = cv.fit(trainData)

    // Make predictions.
    val predictions = model.transform(testData)

    predictions.show(5)

    // Select (prediction, true label) and compute test error.
    val evaluator = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("rmse")
    val rmse = evaluator.evaluate(predictions)
    println(s"Root Mean Squared Error (RMSE) on test data = $rmse for country ${countryRow.getAs[String]("Country")}")

    model.save(s"models/perCountryModels/GBTRegressionModel_Country_${countryRow.getAs[Int]("CountryIndex")}.ml")

    //val gbtModel = model.stages(1).asInstanceOf[GBTRegressionModel]
    println(s"Learned regression GBT model for country ${countryRow.getAs[String]("Country")}")
    println(s"Best params for country ${countryRow.getAs[String]("Country")} :\n ${model.bestModel.explainParams()}")
  }


}