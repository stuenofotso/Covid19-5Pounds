package covid19.kaggle

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.GBTRegressor
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, date_add}
import org.apache.spark.sql.types.DoubleType

/** Main class */
object ForecastGBTMyCustomKaggleDataOld {

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

val HORIZON:Int = 40 //en jours

  /*
  try to find a dataset/datasource with...

  Median age tends to affect the cases. So for a city that has a higher median age, the cases tend to increase.


With the rise in tempertaure, the confirmed cases tend to slow down (negative correlation).
 Hence, people in colder environments and cooler climatic conditions are much prone to the transmission of COVID-19.
   */


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




  def learnModelForFeature(groupedDf:DataFrame, featureName:String): Unit = {
    val adjustedGroupedDf = addTimeValColumns(groupedDf.select("Province_State", "Country_Region",  "Date",  featureName)
      , "Date", featureName, "Province_State", "Country_Region", "Date")
      .na.fill(0.0)
      //.withColumn("Country_Province", udf((s1:String, s2:String)=>s1+"_"+s2, StringType)($"Country_Region", $"Province_State"))
      .cache()

    println("adjustedGroupedDf.printSchema()")
    adjustedGroupedDf.printSchema()
    adjustedGroupedDf.show(5)


    val countryProvinceDf = spark.read.format("csv")
      .option("header","false")
      .option("inferSchema", "true")
      .load("kaggle_data/internal/countryIndexCSV")
      .toDF("Country_Region", "Country_RegionIndex", "Province_State", "Province_StateIndex")



    val indexedDf = groupedDf.join(countryProvinceDf, Seq("Country_Region", "Province_State"))
      .drop(countryProvinceDf("Country_Region")).drop(countryProvinceDf("Province_State"))



    val readyDf = //new VectorAssembler().setInputCols(Array("CountryIndex", "Date", "Recovered_Cases","Dead_Cases"))
      new VectorAssembler().setInputCols("Country_RegionIndex"+:"Province_StateIndex"+:indexedDf.columns.filter(_.startsWith(featureName+"_")))
        .setOutputCol("features")
        .transform(indexedDf)
        .withColumnRenamed(featureName, "label")
        .select("features", "label").cache()




    // Split the data into train and test
    //val splits = readyDf.randomSplit(Array(0.99, 0.01), seed = 1234L)
    //val trainData = splits(0)
    //val testData = splits(1)





    // Train a GBT model.
    val gbt = new GBTRegressor()
      .setLabelCol("label")
      .setFeaturesCol("features")
    .setMaxIter(50)




    //Root Mean Squared Error (RMSE) on test data = 212.00467257169575
    /*val  paramGrid = new ParamGridBuilder()
      .addGrid(gbt.maxBins, Array(190, 200, 220,250)) //190
      .addGrid(gbt.maxDepth, Array(8, 10,12)) //10
      .build()*/

    //Root Mean Squared Error (RMSE) on test data = 415.1760577417328
    val  paramGrid = new ParamGridBuilder()
      .addGrid(gbt.maxBins, Array(184))
      .addGrid(gbt.maxDepth, Array(10, 15)) //best 10
      .build()




    // Chain indexer and GBT in a Pipeline.
    //    val pipeline = new Pipeline()
    //      .setStages(Array(gbt))

    val cv = new CrossValidator()
      .setEstimator(gbt)
      .setEvaluator(new RegressionEvaluator())
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(3)
      .setParallelism(50)

    // Train model. This also runs the indexer.
    println(s"begin of model training for $featureName...")
    val model = cv.fit(readyDf)

    // Make predictions.
    //val predictions = model.transform(testData)

    // Select example rows to display.
    //predictions.show(5)

    // Select (prediction, true label) and compute test error.
    /*val evaluator = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("rmse")
    val rmse = evaluator.evaluate(predictions)
    println(s"Root Mean Squared Error (RMSE) on test data for $featureName = $rmse")
*/
    model.save(s"models/KaggleDataModel_v2/GBTRegressionModel_total_cov_$featureName.ml")

    println(s"Best params for feature $featureName :\n ${model.bestModel.explainParams()}")

  }


  /** Main function */
  def main(args: Array[String]): Unit = {


    val df = spark.read.format("csv")
      .option("header","true")
      .option("inferSchema", "true")
      .load("kaggle_data/train.csv")
      .drop("Id")
      .withColumn("ConfirmedCases",$"ConfirmedCases".cast(DoubleType))
      .withColumn("Fatalities",$"Fatalities".cast(DoubleType))
        .na.fill(0.0, Seq("ConfirmedCases", "Fatalities"))
      .na.fill("RAS", Seq("Province_State"))




    val ndf = addNtoColumnsName(df.withColumn("Date",date_add($"Date",1)), df.columns.toVector)

    val pgroupedDf =  df.join(
      ndf,
      df("Country_Region") <=> ndf("N_Country_Region")
        && df("Province_State") <=> ndf("N_Province_State")
        && df("Date") <=> ndf("N_Date"),
      "left_outer"
    )


    val groupedDf = pgroupedDf
      .withColumn("ConfirmedCases", $"ConfirmedCases"-$"N_ConfirmedCases")
      .withColumn("Fatalities", $"Fatalities"-$"N_Fatalities")
      .na.fill(0.0, Seq("ConfirmedCases", "Fatalities"))
      .select(df("Country_Region"), df("Province_State"), df("Date"), col("ConfirmedCases"), col("Fatalities"))



    println("groupedDf.printSchema()")
    groupedDf.printSchema()
    groupedDf.show(10)



    Seq("ConfirmedCases", "Fatalities").par.foreach(learnModelForFeature(groupedDf, _))



  }


}
