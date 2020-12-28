package graph

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

@Visual(id = "ReportDatasetOutput", label = "ReportDatasetOutput", x = 373, y = 154, phase = 0)
object ReportDatasetOutput {

  @UsesDataset(id = "5", version = 1)
  def apply(spark: SparkSession, in: DataFrame): Target = {
    import spark.implicits._

    val fabric = Config.fabricName
    fabric match {
      case "dp" =>
        val schemaArg = StructType(
          Array(
            StructField("id",           IntegerType, false),
            StructField("report_title", StringType,  false),
            StructField("customers",    IntegerType, false),
            StructField("amount_total", DoubleType,  false),
            StructField("orders_total", IntegerType, false)
          )
        )
        in.write
          .format("csv")
          .option("sep", ",")
          .mode("overwrite")
          .save("dbfs:/Users/visa3/jane/ReportDatasetOutput.csv")
      case _ => throw new Exception("Unknown Fabric")
    }

  }

}
