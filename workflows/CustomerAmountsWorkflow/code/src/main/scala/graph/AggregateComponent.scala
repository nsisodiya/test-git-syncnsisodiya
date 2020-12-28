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

@Visual(id = "AggregateComponent", label = "AggregateComponent", x = 373, y = 98, phase = 0)
object AggregateComponent {

  def apply(spark: SparkSession, in: DataFrame): Aggregate = {
    import spark.implicits._

    val dfGroupBy = in.groupBy(col("order_id").as("order_id"))
    val out = dfGroupBy.agg(
      count(col("order_id")).as("orders"),
      sum(col("amount")).as("amount"),
      max(col("customer_id")).as("customer_id"),
      max(col("first_name")).as("first_name"),
      max(col("last_name")).as("last_name"),
      max(col("phone")).as("phone"),
      max(col("email")).as("email"),
      max(col("country_code")).as("country_code"),
      max(col("account_length_days")).as("account_length_days"),
      max(col("account_flags")).as("account_flags")
    )

    out

  }

}
