package frauddetector

import org.apache.spark.sql.SparkSession
import frauddetector.config.AppConfig
import frauddetector.ingestion.GoldPipeline

object Main {
  def createSparkSession(): SparkSession = {
    val spark = SparkSession.builder()
      .appName("FraudPatternDetector")
      .master("local[*]")
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()

    // Use RawLocalFileSystem to bypass POSIX CRC files on local Windows filesystem
    spark.sparkContext.hadoopConfiguration.set("fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
    spark
  }

  def main(args: Array[String]): Unit = {
    val spark = createSparkSession()
    try {
      println("=== Fraud Pattern Detector — Scala 3 & Apache Spark 3.5 Initialized ===")
      val config = AppConfig.defaultConfig()

      val goldPipeline = new GoldPipeline(spark, config)
      val result = goldPipeline.run()

      println(s"Summary: Scored Transactions=${result.scoredTransactions.count()}, Fraud Alerts=${result.fraudAlerts.count()}")
    } finally {
      spark.stop()
    }
  }
}
