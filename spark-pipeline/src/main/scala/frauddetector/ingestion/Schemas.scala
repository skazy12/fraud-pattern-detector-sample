package frauddetector.ingestion

import org.apache.spark.sql.types._

object Schemas {

  val customersRawSchema: StructType = StructType(Seq(
    StructField("customer_id", StringType, nullable = true),
    StructField("home_country", StringType, nullable = true),
    StructField("risk_profile", StringType, nullable = true),
    StructField("created_at", StringType, nullable = true)
  ))

  val devicesRawSchema: StructType = StructType(Seq(
    StructField("device_id", StringType, nullable = true),
    StructField("customer_id", StringType, nullable = true),
    StructField("device_type", StringType, nullable = true),
    StructField("registered_at", StringType, nullable = true)
  ))

  val transactionsRawSchema: StructType = StructType(Seq(
    StructField("transaction_id", StringType, nullable = true),
    StructField("customer_id", StringType, nullable = true),
    StructField("device_id", StringType, nullable = true),
    StructField("transaction_timestamp", StringType, nullable = true),
    StructField("amount", StringType, nullable = true),
    StructField("currency", StringType, nullable = true),
    StructField("merchant_id", StringType, nullable = true),
    StructField("merchant_category", StringType, nullable = true),
    StructField("country", StringType, nullable = true),
    StructField("status", StringType, nullable = true)
  ))
}
