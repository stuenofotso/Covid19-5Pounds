package covid19

import java.text.SimpleDateFormat

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.GBTRegressor
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, IntegerType, LongType, StringType, StructField, StructType}
import org.elasticsearch.hadoop.cfg.ConfigurationOptions
import org.elasticsearch.hadoop.serialization.Parser
import org.elasticsearch.spark.sql.{EsSparkSQL, ScalaRowValueReader}

import scala.collection.mutable





/**
  * Custom reader that has the same behavior than the elasticsearch-spark reader except for the field date
  * The elasticsearch-spark reader does not take into account the schema of ES especially the format of dates.
  * All dates are considered "date_optional_time"
  **/
class SpecificBasicDateTimeReader extends ScalaRowValueReader {
  override def date(value: String, parser: Parser): AnyRef = {
    parser.currentName() match {
      case "Date" =>
        new java.sql.Timestamp(new SimpleDateFormat("dd-MM-yyyy HH:mm:ss").parse(value).getTime).asInstanceOf[AnyRef]
      case x =>
        super.date(value, parser)
    }
  }
}



/** Main class */
object ESDataPuller {

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
    //val url = "ec2-3-135-203-125.us-east-2.compute.amazonaws.com:9200,ec2-13-58-53-1.us-east-2.compute.amazonaws.com:9200,ec2-18-220-232-235.us-east-2.compute.amazonaws.com:9200,ec2-18-224-63-68.us-east-2.compute.amazonaws.com:9200"
    val url = "localhost:9200"

    println(s"Loading index ${elasticIndex} at : ${url} ...")

    val df = EsSparkSQL.esDF(spark.sqlContext,
      Map(
        ConfigurationOptions.ES_RESOURCE_READ -> elasticIndex,
        ConfigurationOptions.ES_INDEX_READ_MISSING_AS_EMPTY -> "true",
        ConfigurationOptions.ES_READ_FIELD_EMPTY_AS_NULL ->"true",
        ConfigurationOptions.ES_READ_FIELD_AS_ARRAY_INCLUDE ->"Province",
        ConfigurationOptions.ES_NET_HTTP_AUTH_USER->"elastic",
        ConfigurationOptions.ES_NET_HTTP_AUTH_PASS->"W|Hed%/E$]=(",
        ConfigurationOptions.ES_NODES_WAN_ONLY->"true",
        ConfigurationOptions.ES_NODES->url,
        "es.read.field.include" -> "Country, Province,Confirmed,Deaths, Recovered,Date",
        // Option to specify a serialization reader
        ConfigurationOptions.ES_SERIALIZATION_READER_VALUE_CLASS -> classOf[SpecificBasicDateTimeReader].getCanonicalName
      ))
      .withColumn("Province", udf((x:mutable.WrappedArray[String])=>if(x.nonEmpty)x.head else "RAS", StringType)($"Province"))
      .groupBy($"Country", $"Province", $"Date").agg(
        expr("sum(Confirmed) as Confirmed")
      , expr("sum(Deaths) as Deaths")
      , expr("sum(Recovered) as Recovered")
    )


    println("df.printSchema()")
    df.printSchema()
    df.show(5)



    val ndf = df.withColumn("nextDate",date_add($"Date",1))
      .withColumn("NConfirmed", $"Confirmed")
      .withColumn("NDeaths", $"Deaths")
      .withColumn("NRecovered", $"Recovered")
      .select("Country", "Province", "NConfirmed","NDeaths", "NRecovered", "nextDate")

    val pgroupedDf =  df.join(
                      ndf,
                      df("Country") <=> ndf("Country")
                        && df("Province") <=> ndf("Province")
                        && df("Date") <=> ndf("nextDate"),
                      "left_outer"
                    )
              .withColumn("DateInt", (dayofmonth(df("Date"))/1.3)+(lit(30) * month(df("Date"))))
              .cache()

    val groupedDf = pgroupedDf
                    .withColumn("Confirmed", $"Confirmed"-$"NConfirmed")
                    .withColumn("Deaths", $"Deaths"-$"NDeaths")
                    .withColumn("Recovered", $"Recovered"-$"NRecovered") //udf((x:Long)=>if(x != null) x else 0, LongType)(odf("Recovered"))
                    .select(df("Country"), df("Province"), $"Date", $"DateInt",  $"Confirmed", $"Deaths", $"Recovered")

    println("groupedDf.printSchema()")
    groupedDf.printSchema()
    groupedDf.show(10)

    groupedDf.write.mode(SaveMode.Overwrite).format("csv").save("data/grouped_coviddatacsv")

    merge("data/grouped_coviddatacsv", "data/grouped_coviddata.csv")
  }

  def merge(srcPath: String, dstPath: String): Unit =  {
    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    FileUtil.copyMerge(hdfs, new Path(srcPath), hdfs, new Path(dstPath), false, hadoopConfig, null)
  }


}