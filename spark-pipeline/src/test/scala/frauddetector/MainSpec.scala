package frauddetector

import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

class MainSpec extends AnyFunSuite {
  test("SparkSession initializes cleanly in UTC and executes DataFrame operations") {
    val spark = Main.createSparkSession()
    try {
      assert(spark.conf.get("spark.sql.session.timeZone") == "UTC")

      val schema = StructType(Seq(
        StructField("customer_id", StringType, nullable = false),
        StructField("home_country", StringType, nullable = false),
        StructField("risk_profile", StringType, nullable = false)
      ))
      val rows = java.util.Arrays.asList(
        Row("C000001", "ES", "LOW"),
        Row("C000002", "FR", "HIGH")
      )
      val df = spark.createDataFrame(rows, schema)

      assert(df.count() == 2)
      assert(df.filter("home_country = 'ES'").count() == 1)
    } finally {
      spark.stop()
    }
  }
}
