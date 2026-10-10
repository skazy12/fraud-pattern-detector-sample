package frauddetector.quality

import org.scalatest.funsuite.AnyFunSuite
import frauddetector.Main
import frauddetector.config.AppConfig
import frauddetector.ingestion.SilverPipeline

class DataQualityValidationSpec extends AnyFunSuite {

  test("SilverPipeline & DataQualityValidation processes sample batch with full quality catalog & reconciliation") {
    val spark = Main.createSparkSession()
    import spark.implicits._
    try {
      val testConfig = AppConfig(
        batchId = "test_silver_01",
        rawInputDir = "../data/sample",
        bronzeOutputDir = "../data/bronze",
        silverOutputDir = "../data/silver",
        quarantineOutputDir = "../data/quarantine"
      )

      val pipeline = new SilverPipeline(spark, testConfig)
      val result = pipeline.run()

      val metrics = result.metrics

      // Verify Source Row Count
      assert(metrics.sourceRowCount == 19L)

      // Verify Valid Unique Row Count (13 transactions enter Silver: 19 total - 1 exact dupe - 2 conflicting dupes - 3 invalid field rows = 13)
      assert(metrics.validUniqueRowCount == 13L)

      // Verify Exact Duplicate Count (1 copy removed under Q009A)
      assert(metrics.exactDuplicateCount == 1L)

      // Verify Conflicting Duplicate Count (2 rows quarantined under Q009B for T000002)
      assert(metrics.conflictingDuplicateCount == 2L)

      // Verify Semantic Rejections (3 rows: Q004, Q005, Q006)
      assert(metrics.semanticRejectionCount == 3L)

      // Verify Total Reconciliation: 14 valid + 1 exact dupe removed + 5 quarantined rows (2 conflicting + 3 field errors) = 19
      val totalQuarantinedRows = result.quarantine.count()
      assert(totalQuarantinedRows == 5L)
      assert(metrics.sourceRowCount == metrics.validUniqueRowCount + metrics.exactDuplicateCount + totalQuarantinedRows)

      // Verify Enriched Transactions Schema and Attributes
      val enrichedDF = result.enrichedTransactions
      val cols = enrichedDF.columns.toSet
      assert(cols.contains("transaction_id"))
      assert(cols.contains("home_country"))
      assert(cols.contains("risk_profile"))
      assert(cols.contains("known_device"))

      // Verify known_device boolean logic
      val knownTx = enrichedDF.filter($"transaction_id" === "T000001").select("known_device").collect().head.getBoolean(0)
      assert(knownTx == true, "Registered device D000001 for C000001 must produce known_device=true")

      val unknownTx = enrichedDF.filter($"transaction_id" === "T000501").select("known_device").collect().head.getBoolean(0)
      assert(unknownTx == false, "Unregistered device D999999 for C000005 must produce known_device=false")

      // Verify Quarantine Error Codes
      val errorCodes = result.quarantine.select("primary_error_code").distinct().collect().map(_.getString(0)).toSet
      assert(errorCodes.contains("Q004"), "Quarantine must contain Q004 (invalid amount)")
      assert(errorCodes.contains("Q005"), "Quarantine must contain Q005 (unsupported currency)")
      assert(errorCodes.contains("Q006"), "Quarantine must contain Q006 (unsupported country)")
      assert(errorCodes.contains("Q009B"), "Quarantine must contain Q009B (conflicting duplicate)")

    } finally {
      spark.stop()
    }
  }
}
