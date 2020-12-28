import org.apache.spark.sql.types._
import io.prophecy.libs._
import io.prophecy.libs.UDFUtils._
import io.prophecy.libs.Component._
import io.prophecy.libs.DataHelpers._
import io.prophecy.libs.SparkFunctions._
import io.prophecy.libs.FixedFileFormatImplicits._
import org.apache.spark.sql.ProphecyDataFrame._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import config.ConfigStore._

import graph._

@Visual(mode = "batch")
object Main {

  def graph(spark: SparkSession): Unit = {

    val df_CustomerOrdersDatasetInput: Source   = CustomerOrdersDatasetInput(spark)
    val df_PrepareComponent:           Reformat = PrepareComponent(spark, df_CustomerOrdersDatasetInput)
    FinalDatasetOutput(spark, df_PrepareComponent)
    val df_AggReportComponent: Aggregate = AggReportComponent(spark, df_PrepareComponent)
    ReportDatasetOutput(spark, df_AggReportComponent)

  }

  def main(args: Array[String]): Unit = {
    import config._
    ConfigStore.Config = ConfigurationFactoryImpl.fromCLI(args)

    val spark: SparkSession = SparkSession
      .builder()
      .appName("PrepareForBank")
      .config("spark.default.parallelism", 4)
      .enableHiveSupport()
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setCheckpointDir("/tmp/checkpoints")

    graph(spark)
  }

}
