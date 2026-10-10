package frauddetector.engine

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import frauddetector.config.AppConfig

case class FraudEngineResult(
  scoredTransactions: DataFrame,
  fraudAlerts: DataFrame
)

class FraudRulesEngine(spark: SparkSession, config: AppConfig) {
  import spark.implicits._

  def evaluate(enrichedTransactions: DataFrame): FraudEngineResult = {
    println(s"=== Running Fraud Pattern Detection Engine for Batch: ${config.batchId} ===")

    val tsCol = col("transaction_timestamp")
    val tsLongCol = tsCol.cast("long")

    // --- R001: Transaction Burst (10 min window, APPROVED only, >= 5) ---
    val windowBurst = Window
      .partitionBy("customer_id")
      .orderBy(tsLongCol)
      .rangeBetween(-600, 0) // 10 minutes = 600 seconds

    val approvedTx = enrichedTransactions
      .withColumn("is_approved_int", when(col("status") === "APPROVED", 1).otherwise(0))

    val withR001Count = approvedTx
      .withColumn("approved_burst_count", sum("is_approved_int").over(windowBurst))
      .withColumn("rule_r001", when(col("status") === "APPROVED" && col("approved_burst_count") >= 5, true).otherwise(false))

    // --- R002: High Amount (by risk_profile threshold) ---
    // LOW: > 2000, MEDIUM: > 1500, HIGH: > 1000
    val highAmountThreshold = when(col("risk_profile") === "LOW", 2000.00)
      .when(col("risk_profile") === "MEDIUM", 1500.00)
      .when(col("risk_profile") === "HIGH", 1000.00)
      .otherwise(999999999.00)

    val withR002 = withR001Count
      .withColumn("rule_r002", when(col("status") === "APPROVED" && col("amount") > highAmountThreshold, true).otherwise(false))

    // --- R003: Rapid Country Change (lag(1) over APPROVED tx for same customer within 60 min) ---
    val windowApprovedLag = Window
      .partitionBy("customer_id")
      .orderBy(tsCol.asc, col("transaction_id").asc)

    val approvedOnlyDF = withR002
      .filter(col("status") === "APPROVED")
      .withColumn("prev_country", lag("country", 1).over(windowApprovedLag))
      .withColumn("prev_ts", lag(tsCol, 1).over(windowApprovedLag))
      .withColumn("elapsed_sec", tsLongCol - col("prev_ts").cast("long"))
      .withColumn("rule_r003_calc", when(
        col("prev_country").isNotNull &&
        col("country") =!= col("prev_country") &&
        col("elapsed_sec") >= 0 &&
        col("elapsed_sec") <= 3600, // 60 minutes = 3600 seconds
        true
      ).otherwise(false))
      .select(col("transaction_id").as("app_tx_id"), col("rule_r003_calc"))

    val withR003 = withR002
      .join(approvedOnlyDF, col("transaction_id") === col("app_tx_id"), "left")
      .withColumn("rule_r003", coalesce(col("rule_r003_calc"), lit(false)))
      .drop("app_tx_id", "rule_r003_calc")

    // --- R004: Repeated Declines (15 min window, DECLINED only, >= 3) ---
    val windowDeclines = Window
      .partitionBy("customer_id")
      .orderBy(tsLongCol)
      .rangeBetween(-900, 0) // 15 minutes = 900 seconds

    val declinedTx = withR003
      .withColumn("is_declined_int", when(col("status") === "DECLINED", 1).otherwise(0))
      .withColumn("declined_count", sum("is_declined_int").over(windowDeclines))
      .withColumn("rule_r004", when(col("status") === "DECLINED" && col("declined_count") >= 3, true).otherwise(false))

    // --- R005: Unknown Device with Additional Indicator ---
    val withR005 = declinedTx
      .withColumn("rule_r005", when(!col("known_device") && (col("rule_r001") || col("rule_r002") || col("rule_r003") || col("rule_r004")), true).otherwise(false))

    // --- Risk Score & Risk Level Calculation ---
    val scoreExpr = (when(col("rule_r001"), 20).otherwise(0) +
      when(col("rule_r002"), 30).otherwise(0) +
      when(col("rule_r003"), 40).otherwise(0) +
      when(col("rule_r004"), 15).otherwise(0) +
      when(col("rule_r005"), 10).otherwise(0))

    val levelExpr = when(col("risk_score") >= 60, "CRITICAL")
      .when(col("risk_score") >= 40, "HIGH")
      .when(col("risk_score") >= 20, "MEDIUM")
      .otherwise("LOW")

    val triggeredRulesExpr = array_remove(array(
      when(col("rule_r001"), lit("R001")).otherwise(null),
      when(col("rule_r002"), lit("R002")).otherwise(null),
      when(col("rule_r003"), lit("R003")).otherwise(null),
      when(col("rule_r004"), lit("R004")).otherwise(null),
      when(col("rule_r005"), lit("R005")).otherwise(null)
    ), null)

    val scoredTransactions = withR005
      .withColumn("risk_score", scoreExpr)
      .withColumn("risk_level", levelExpr)
      .withColumn("triggered_rules", triggeredRulesExpr)
      .select(
        col("batch_id"),
        col("transaction_id"),
        col("customer_id"),
        col("transaction_timestamp"),
        col("amount"),
        col("status"),
        col("country"),
        col("risk_profile"),
        col("known_device"),
        col("rule_r001"),
        col("rule_r002"),
        col("rule_r003"),
        col("rule_r004"),
        col("rule_r005"),
        col("risk_score"),
        col("risk_level"),
        col("triggered_rules")
      )

    val fraudAlerts = scoredTransactions
      .filter(col("risk_score") > 0)
      .withColumn("alert_id", concat_ws("_", lit("ALT"), col("batch_id"), col("transaction_id")))

    println(s"Engine Complete: Scored Transactions=${scoredTransactions.count()} | Fraud Alerts=${fraudAlerts.count()}")

    FraudEngineResult(
      scoredTransactions = scoredTransactions,
      fraudAlerts = fraudAlerts
    )
  }
}
