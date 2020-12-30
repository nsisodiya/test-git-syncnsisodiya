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

  @UsesDataset(id = "3", version = 0)
  def apply(spark: SparkSession): Source = {
    import spark.implicits._

    val fabric = Config.fabricName

    lazy val out = fabric match {
      case "dp" =>
        val schemaArg = StructType(
          Array(
            StructField("order_id",            IntegerType, false),
            StructField("orders",              IntegerType, false),
            StructField("amount",              DoubleType,  false),
            StructField("customer_id",         IntegerType, false),
            StructField("first_name",          StringType,  false),
            StructField("last_name",           StringType,  false),
            StructField("phone",               StringType,  false),
            StructField("email",               StringType,  false),
            StructField("country_code",        StringType,  false),
            StructField("account_length_days", IntegerType, false),
            StructField("account_flags",       StringType,  false)
          )
        )
        spark.read
          .format("csv")
          .option("header", true)
          .option("sep",    ",")
          .schema(schemaArg)
          .load("file:///storage/livy/data/OrdersDatasetInput.csv")
          .cache()
      case _ => throw new Exception(s"The fabric '$fabric' is not handled")
    }

    out

  }

}
