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

@Visual(id = "PrepareComponent", label = "PrepareComponent", x = 251, y = 98, phase = 0)
object PrepareComponent {

  def apply(spark: SparkSession, in: DataFrame): Reformat = {
    import spark.implicits._

    val out = in.select(
      datediff(current_date(), col("account_open_date")).as("account_length_days"),
      col("order_id").as("order_id"),
      col("customer_id").as("customer_id"),
      col("amount").as("amount"),
      col("first_name").as("first_name"),
      col("last_name").as("last_name"),
      col("phone").as("phone"),
      col("email").as("email"),
      col("country_code").as("country_code"),
      col("account_flags").as("account_flags")
    )

    out

  }

}
