package frauddetector.exporter

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.SparkSession
import frauddetector.config.AppConfig
import frauddetector.ingestion.GoldPipeline
import java.nio.file.{Files, Paths}

class DashboardExporterSpec extends AnyFunSuite with Matchers {

  test("DashboardExporter exports valid JSON summary files for batch_sample_01") {
    val spark = SparkSession
      .builder()
      .appName("DashboardExporterSpec")
      .master("local[*]")
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()

    val config = AppConfig(batchId = "batch_sample_01", rawInputDir = "../data/sample")
    val pipeline = new GoldPipeline(spark, config)
    pipeline.run()

    val exportDir = Paths.get(config.dashboardExportDir)
    val rootDataDir = Paths.get(config.dashboardRootDataDir)

    // Check files exist in both export targets
    val expectedFiles = Seq("summary_kpis.json", "rule_statistics.json", "customer_risk_summary.json", "fraud_alerts.json", "export_manifest.json")

    expectedFiles.foreach { fileName =>
      val p1 = exportDir.resolve(fileName)
      val p2 = rootDataDir.resolve(fileName)

      Files.exists(p1) shouldBe true
      Files.exists(p2) shouldBe true

      val content = new String(Files.readAllBytes(p1), "UTF-8")
      content should not be empty
    }

    // Verify summary KPIs content
    val kpisContent = new String(Files.readAllBytes(exportDir.resolve("summary_kpis.json")), "UTF-8")
    kpisContent should include("batch_sample_01")
    kpisContent should include("total_scored_transactions")
    kpisContent should include("total_fraud_alerts")
    kpisContent should include("CRITICAL")

    // Verify rule statistics content
    val ruleStatsContent = new String(Files.readAllBytes(exportDir.resolve("rule_statistics.json")), "UTF-8")
    ruleStatsContent should include("R001")
    ruleStatsContent should include("R002")
    ruleStatsContent should include("R003")
    ruleStatsContent should include("R004")
    ruleStatsContent should include("R005")

    spark.stop()
  }
}
