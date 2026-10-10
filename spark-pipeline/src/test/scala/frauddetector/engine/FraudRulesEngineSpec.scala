package frauddetector.engine

import org.scalatest.funsuite.AnyFunSuite
import frauddetector.Main
import frauddetector.config.AppConfig
import frauddetector.ingestion.GoldPipeline

class FraudRulesEngineSpec extends AnyFunSuite {

  test("GoldPipeline & FraudRulesEngine evaluates R001-R005 and generates scored transactions and alerts") {
    val spark = Main.createSparkSession()
    import spark.implicits._

    try {
      val testConfig = AppConfig(
        batchId = "test_gold_01",
        rawInputDir = "../data/sample",
        bronzeOutputDir = "../data/bronze",
        silverOutputDir = "../data/silver",
        quarantineOutputDir = "../data/quarantine"
      )

      val pipeline = new GoldPipeline(spark, testConfig)
      val result = pipeline.run()

      val scoredDF = result.scoredTransactions
      val alertsDF = result.fraudAlerts

      // 1. Verify Scored Transactions Row Count (13 valid unique transactions)
      assert(scoredDF.count() == 13L)

      // 2. Verify R001 Trigger (Burst: C000001 T000105 5th tx in 8 min)
      val r001Triggered = scoredDF.filter($"transaction_id" === "T000105").select("rule_r001").collect().head.getBoolean(0)
      assert(r001Triggered == true, "T000105 must trigger R001 (Burst)")

      // 3. Verify R002 Trigger (High Amount: C000002 T000201 amount 2500 EUR > 1500 EUR MEDIUM threshold)
      val r002Triggered = scoredDF.filter($"transaction_id" === "T000201").select("rule_r002").collect().head.getBoolean(0)
      assert(r002Triggered == true, "T000201 must trigger R002 (High Amount)")

      // 4. Verify R003 Trigger (Rapid Country Change: C000003 T000302 ES -> FR in 35 min)
      val r003Triggered = scoredDF.filter($"transaction_id" === "T000302").select("rule_r003").collect().head.getBoolean(0)
      assert(r003Triggered == true, "T000302 must trigger R003 (Rapid Country Change)")

      // 5. Verify R004 Trigger (Repeated Declines: C000004 T000403 3rd decline in 8 min)
      val r004Triggered = scoredDF.filter($"transaction_id" === "T000403").select("rule_r004").collect().head.getBoolean(0)
      assert(r004Triggered == true, "T000403 must trigger R004 (Repeated Declines)")

      // 6. Verify R005 Trigger (Unknown Device + Additional Indicator: C000005 T000501 amount 1800 > 1000 HIGH profile + unknown device D999999)
      val r005Row = scoredDF.filter($"transaction_id" === "T000501").select("rule_r002", "rule_r005", "risk_score", "risk_level").collect().head
      assert(r005Row.getBoolean(0) == true, "T000501 must trigger R002")
      assert(r005Row.getBoolean(1) == true, "T000501 must trigger R005")
      assert(r005Row.getInt(2) == 40, "T000501 score must be 30(R002) + 10(R005) = 40")
      assert(r005Row.getString(3) == "HIGH", "Score 40 must map to HIGH risk level")

      // 7. Verify Fraud Alerts Count and Alert ID derivation
      assert(alertsDF.count() > 0)
      val sampleAlertId = alertsDF.filter($"transaction_id" === "T000501").select("alert_id").collect().head.getString(0)
      assert(sampleAlertId == "ALT_test_gold_01_T000501", "Alert ID must format as ALT_<batch_id>_<transaction_id>")

    } finally {
      spark.stop()
    }
  }
}
