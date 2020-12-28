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

@Visual(id = "AggReportComponent", label = "AggReportComponent", x = 251, y = 154, phase = 0)
object AggReportComponent {

  def apply(spark: SparkSession, in: DataFrame): Aggregate = {
    import spark.implicits._

    val dfGroupBy = in.groupBy(col("id").as("id"))
    val out = dfGroupBy.agg(
      lit("VAT_Summary_Report").as("report_title"),
      count(col("id")).as("customers"),
      sum(col("amount")).as("amount_total"),
      sum(col("orders")).as("orders_total")
    )

    out

  }

}
