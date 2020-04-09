package covid19

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.GBTRegressor
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.date_add
import org.apache.spark.sql.types.LongType


/** Main class */
object ForecastGBTMyCustom_short {

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


    println("groupedDf.printSchema()")
    groupedDf.printSchema()
    groupedDf.show(10)


    val adjustedGroupedDf = addTimeValColumns(groupedDf.select("Country",  "Date",  "Confirmed")
      , "Date", "Confirmed", "Country", "Date")
      .na.fill(0)
      .cache()

    println("adjustedGroupedDf.printSchema()")
    adjustedGroupedDf.printSchema()
    adjustedGroupedDf.show(10)




    val indexedDf = new StringIndexer()
      .setInputCol("Country")
      .setOutputCol("CountryIndex")
      .fit(adjustedGroupedDf)
      .transform(adjustedGroupedDf)


    val featureColums = "CountryIndex"+:indexedDf.columns.filter(_.startsWith("Confirmed_"))


    val readyDf = //new VectorAssembler().setInputCols(Array("CountryIndex", "Date", "Recovered_Cases","Dead_Cases"))
              new VectorAssembler().setInputCols(featureColums)
                      .setOutputCol("features")
                      .transform(indexedDf)
                      .withColumn("label", $"Confirmed")
                      .select("features", "label").cache()



    readyDf.printSchema()

    // Split the data into train and test
    val splits = readyDf.randomSplit(Array(0.9, 0.1), seed = 1234L)
    val trainData = splits(0)
    val testData = splits(1)





    // Train a GBT model.
    val gbt = new GBTRegressor()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setMaxIter(10)




    //Root Mean Squared Error (RMSE) on test data = 212.00467257169575
    /*val  paramGrid = new ParamGridBuilder()
      .addGrid(gbt.maxBins, Array(190, 200, 220,250)) //190
      .addGrid(gbt.maxDepth, Array(8, 10,12)) //10
      .build()*/

    //Root Mean Squared Error (RMSE) on test data = 455.6411153813686
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

    model.save("models/GBTRegressionModel_total_cov_v3_short.ml")

    //val gbtModel = model.stages(1).asInstanceOf[GBTRegressionModel]
    println(s"Learned regression GBT model:\n ${model.toString()}")

    println(s"Best params :\n ${model.bestModel.explainParams()}")


  }


}