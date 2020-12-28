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

@Visual(id = "OrdersDatasetInput", label = "OrdersDatasetInput", x = 7, y = 42, phase = 0)
object OrdersDatasetInput {

  @UsesDataset(id = "2", version = 2)
  def apply(spark: SparkSession): Source = {
    import spark.implicits._

    val fabric = Config.fabricName

    lazy val out = fabric match {
      case "dp" =>
        val schemaArg = StructType(
          Array(
            StructField("order_id",       IntegerType, false),
            StructField("customer_id",    IntegerType, false),
            StructField("order_status",   StringType,  false),
            StructField("order_category", StringType,  false),
            StructField("order_date",     StringType,  false),
            StructField("amount",         DoubleType,  false)
          )
        )
        spark.read
          .format("csv")
          .option("sep", ",")
          .schema(schemaArg)
          .load("dbfs:/Users/visa3/jane/OrdersDatasetInput.csv")
          .cache()
      case _ => throw new Exception(s"The fabric '$fabric' is not handled")
    }

    out

  }

}
