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

@Visual(id = "PrepareComponent", label = "PrepareComponent", x = 129, y = 98, phase = 0)
object PrepareComponent {

  def apply(spark: SparkSession, in: DataFrame): Reformat = {
    import spark.implicits._

    val out = in.select(
      col("customer_id").as("id"),
      concat(col("first_name"), lit(" "), col("last_name")).as("full_name"),
      substring(col("phone"),   2,        10).as("phone"),
      substring(col("phone"),   0,        2).as("phone_area_code"),
      col("email").as("email"),
      col("email").as("email_provider"),
      col("orders").as("orders"),
      col("amount").as("amount"),
      when(substring(col("account_flags"), 1, 1) === "D", lit("Y")).otherwise(lit("N")).as("delinquent_last_90_days")
    )

    out

  }

}
