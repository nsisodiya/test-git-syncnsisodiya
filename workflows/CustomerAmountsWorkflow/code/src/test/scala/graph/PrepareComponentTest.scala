
package graph

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import io.prophecy.libs.SparkTestingUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class PrepareComponentTest extends FunSuite with DataFrameSuiteBase {
  import sqlContext.implicits._

  test("Test Unit test 0 for out columns: order_id") {
    val dfIn = inDf(Seq("order_id"), Seq(
      Seq("2")
    ))
    val dfOut = outDf(Seq("order_id"), Seq(
      Seq("2")
    ))

    val dfOutComputed = graph.PrepareComponent(spark, dfIn)
    assertDFEquals(dfOut.select("order_id"), dfOutComputed.select("order_id"))
  }

  def inDf(columns: Seq[String], values: Seq[Seq[String]]): DataFrame = {
    val inSchema = StructType(List(
      StructField("account_open_date", StringType, nullable = true),
        StructField("order_id", StringType, nullable = true),
        StructField("customer_id", StringType, nullable = true),
        StructField("amount", StringType, nullable = true),
        StructField("first_name", StringType, nullable = true),
        StructField("last_name", StringType, nullable = true),
        StructField("phone", StringType, nullable = true),
        StructField("email", StringType, nullable = true),
        StructField("country_code", StringType, nullable = true),
        StructField("account_flags", StringType, nullable = true)
    ))


    val defaults = Map[String, Any](
      "account_open_date" -> "",
      "order_id" -> "",
      "customer_id" -> "",
      "amount" -> "",
      "first_name" -> "",
      "last_name" -> "",
      "phone" -> "",
      "email" -> "",
      "country_code" -> "",
      "account_flags" -> ""
    )

    val missingColumns = (defaults.keySet -- columns.toSet).toList
    val allColumns     = columns ++ missingColumns
    val allValues      = values.map { row => row ++ missingColumns.map(column => defaults(column)) }

    val rdd    = spark.sparkContext.parallelize(allValues.map(values => Row.fromSeq(values)))
    val schema = StructType(allColumns.map(column => StructField(column, StringType)))
    val df     = spark.createDataFrame(rdd, schema)

    df
  }

  def outDf(columns: Seq[String], values: Seq[Seq[String]]): DataFrame = {
    val outSchema = StructType(List(
      StructField("account_length_days", StringType, nullable = true),
        StructField("order_id", StringType, nullable = true),
        StructField("customer_id", StringType, nullable = true),
        StructField("amount", StringType, nullable = true),
        StructField("first_name", StringType, nullable = true),
        StructField("last_name", StringType, nullable = true),
        StructField("phone", StringType, nullable = true),
        StructField("email", StringType, nullable = true),
        StructField("country_code", StringType, nullable = true),
        StructField("account_flags", StringType, nullable = true)
    ))


    val defaults = Map[String, Any](
      "account_length_days" -> "",
      "order_id" -> "",
      "customer_id" -> "",
      "amount" -> "",
      "first_name" -> "",
      "last_name" -> "",
      "phone" -> "",
      "email" -> "",
      "country_code" -> "",
      "account_flags" -> ""
    )

    val missingColumns = (defaults.keySet -- columns.toSet).toList
    val allColumns     = columns ++ missingColumns
    val allValues      = values.map { row => row ++ missingColumns.map(column => defaults(column)) }

    val rdd    = spark.sparkContext.parallelize(allValues.map(values => Row.fromSeq(values)))
    val schema = StructType(allColumns.map(column => StructField(column, StringType)))
    val df     = spark.createDataFrame(rdd, schema)

    df
  }


  /**
   * Compares if two [[DataFrame]]s are equal, checks that the schemas are the same.
   * When comparing inexact fields uses tol.
   *
   * @param tol max acceptable tolerance, should be less than 1.
   */
  def assertDFEquals(expected: DataFrame, result: DataFrame, tol: Double = 0.0) {
    try {
      expected.rdd.cache
      result.rdd.cache
      assert("Length not Equal", expected.rdd.count, result.rdd.count)

      val expectedIndexValue = expected.rdd.zipWithIndex().map { case (row, idx) => (idx, row) }
      val resultIndexValue   = result.rdd.zipWithIndex().map { case (row, idx) => (idx, row) }

      val unequalRDD = expectedIndexValue
        .join(resultIndexValue)
        .filter { case (idx, (r1, r2)) => !SparkTestingUtils.rowEquals(r1, r2, tol) }

      assertEmpty(unequalRDD.take(maxUnequalRowsToShow))
    } finally {
      expected.rdd.unpersist()
      result.rdd.unpersist()
    }
  }
}

