package frauddetector.ingestion

import org.scalatest.funsuite.AnyFunSuite
import frauddetector.Main
import frauddetector.config.AppConfig
import java.io.File

class BronzeIngestionSpec extends AnyFunSuite {

  test("BronzeIngestion ingests raw sample CSVs and persists metadata-enriched Parquet") {
    val spark = Main.createSparkSession()
    try {
      val testConfig = AppConfig(
        batchId = "test_batch_01",
        rawInputDir = "../data/sample",
        bronzeOutputDir = "../data/bronze"
      )

      val ingestion = new BronzeIngestion(spark, testConfig)
      val metrics = ingestion.runAll()

      assert(metrics.size == 3)
      assert(metrics.find(_.datasetName == "customers").get.bronzeRows == 5)
      assert(metrics.find(_.datasetName == "devices").get.bronzeRows == 5)
      assert(metrics.find(_.datasetName == "transactions").get.bronzeRows == 19)

      // Verify Parquet output directory exists
      val bronzeTxPath = testConfig.transactionsBronzePath
      val parquetDir = new File(bronzeTxPath)
      assert(parquetDir.exists() && parquetDir.isDirectory, s"Parquet output path must exist: $bronzeTxPath")

      // Read back from Bronze Parquet
      val bronzeTxDF = spark.read.parquet(bronzeTxPath)
      assert(bronzeTxDF.count() == 19)

      // Verify Bronze metadata columns exist
      val cols = bronzeTxDF.columns.toSet
      assert(cols.contains("batch_id"))
      assert(cols.contains("source_file"))
      assert(cols.contains("source_row_number"))
      assert(cols.contains("ingested_at_utc"))

      // Verify batch_id value
      val distinctBatchId = bronzeTxDF.select("batch_id").distinct().collect().head.getString(0)
      assert(distinctBatchId == "test_batch_01")
    } finally {
      spark.stop()
    }
  }
}
