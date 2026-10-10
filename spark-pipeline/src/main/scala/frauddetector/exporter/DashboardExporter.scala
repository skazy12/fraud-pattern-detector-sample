package frauddetector.exporter

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import frauddetector.config.AppConfig
import java.nio.file.{Files, Paths, StandardOpenOption}
import java.time.Instant
import java.time.format.DateTimeFormatter

object DashboardExporter {

  def exportMetrics(
      scoredDf: DataFrame,
      alertsDf: DataFrame,
      config: AppConfig
  )(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    val batchId = config.batchId
    val timestamp = DateTimeFormatter.ISO_INSTANT.format(Instant.now())

    // 1. Compute Summary KPIs
    val totalScored = scoredDf.count()
    val totalAlerts = alertsDf.count()
    val alertRatePct = if (totalScored > 0) (totalAlerts.toDouble / totalScored * 100.0) else 0.0

    val riskLevelCounts = scoredDf
      .groupBy("risk_level")
      .count()
      .collect()
      .map(row => row.getAs[String]("risk_level") -> row.getAs[Long]("count"))
      .toMap

    val lowCount = riskLevelCounts.getOrElse("LOW", 0L)
    val mediumCount = riskLevelCounts.getOrElse("MEDIUM", 0L)
    val highCount = riskLevelCounts.getOrElse("HIGH", 0L)
    val criticalCount = riskLevelCounts.getOrElse("CRITICAL", 0L)

    val kpisJson =
      s"""{
         |  "batch_id": "$batchId",
         |  "raw_input_dir": "${config.rawInputDir}",
         |  "exported_at_utc": "$timestamp",
         |  "total_scored_transactions": $totalScored,
         |  "total_fraud_alerts": $totalAlerts,
         |  "alert_rate_pct": ${BigDecimal(alertRatePct).setScale(2, BigDecimal.RoundingMode.HALF_UP)},
         |  "risk_levels": {
         |    "LOW": $lowCount,
         |    "MEDIUM": $mediumCount,
         |    "HIGH": $highCount,
         |    "CRITICAL": $criticalCount
         |  }
         |}""".stripMargin

    // 2. Compute Rule Statistics
    val r001Count = scoredDf.filter($"rule_r001" === true).count()
    val r002Count = scoredDf.filter($"rule_r002" === true).count()
    val r003Count = scoredDf.filter($"rule_r003" === true).count()
    val r004Count = scoredDf.filter($"rule_r004" === true).count()
    val r005Count = scoredDf.filter($"rule_r005" === true).count()

    def pct(c: Long): Double = if (totalScored > 0) BigDecimal(c.toDouble / totalScored * 100.0).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble else 0.0

    val ruleStatsJson =
      s"""[
         |  {"rule_id": "R001", "rule_name": "Velocity Burst (10m)", "points": 20, "activations": $r001Count, "activation_pct": ${pct(r001Count)}},
         |  {"rule_id": "R002", "rule_name": "High Amount Threshold", "points": 30, "activations": $r002Count, "activation_pct": ${pct(r002Count)}},
         |  {"rule_id": "R003", "rule_name": "Rapid Country Change (60m)", "points": 40, "activations": $r003Count, "activation_pct": ${pct(r003Count)}},
         |  {"rule_id": "R004", "rule_name": "Repeated Declines (15m)", "points": 15, "activations": $r004Count, "activation_pct": ${pct(r004Count)}},
         |  {"rule_id": "R005", "rule_name": "Unknown Device Combo", "points": 10, "activations": $r005Count, "activation_pct": ${pct(r005Count)}}
         |]""".stripMargin

    // 3. Customer Risk Summary
    val customerSummaryDf = scoredDf
      .groupBy("customer_id", "risk_profile")
      .agg(
        count("transaction_id").as("total_tx"),
        avg("risk_score").as("avg_score"),
        max("risk_score").as("max_score"),
        sum(when($"risk_score" > 0, 1).otherwise(0)).as("alert_count")
      )

    val customerSummaryJsonRows = customerSummaryDf.collect().map { row =>
      val custId = row.getAs[String]("customer_id")
      val profile = Option(row.getAs[String]("risk_profile")).getOrElse("LOW")
      val totalTx = row.getAs[Long]("total_tx")
      val avgScore = BigDecimal(row.getAs[Double]("avg_score")).setScale(1, BigDecimal.RoundingMode.HALF_UP)
      val maxScore = row.getAs[Int]("max_score")
      val alertCount = row.getAs[Long]("alert_count")
      s"""  {
         |    "customer_id": "$custId",
         |    "customer_name": "$custId",
         |    "risk_profile": "$profile",
         |    "total_transactions": $totalTx,
         |    "avg_risk_score": $avgScore,
         |    "max_risk_score": $maxScore,
         |    "total_alerts": $alertCount
         |  }""".stripMargin
    }
    val customerSummaryJson = "[\n" + customerSummaryJsonRows.mkString(",\n") + "\n]"

    // 4. Fraud Alerts Export
    val alertsJsonRows = alertsDf.collect().map { row =>
      val alertId = row.getAs[String]("alert_id")
      val txId = row.getAs[String]("transaction_id")
      val custId = row.getAs[String]("customer_id")
      val score = row.getAs[Int]("risk_score")
      val level = row.getAs[String]("risk_level")

      val rulesSeq = if (row.isNullAt(row.fieldIndex("triggered_rules"))) Seq.empty[String]
                     else row.getAs[Seq[String]]("triggered_rules")
      val rules = rulesSeq.map(r => s""""$r"""").mkString("[", ", ", "]")

      val rawTs = row.get(row.fieldIndex("transaction_timestamp"))
      val tsStr = rawTs match {
        case ts: java.sql.Timestamp => ts.toInstant.toString
        case s: String => s
        case _ => Instant.now().toString
      }

      val rawAmount = row.get(row.fieldIndex("amount"))
      val amountStr = if (rawAmount == null) "0.00" else rawAmount.toString

      val currency = if (row.schema.fieldNames.contains("currency")) row.getAs[String]("currency") else "EUR"
      val country = row.getAs[String]("country")
      val status = row.getAs[String]("status")
      val knownDevice = row.getAs[Boolean]("known_device")

      s"""  {
         |    "alert_id": "$alertId",
         |    "transaction_id": "$txId",
         |    "customer_id": "$custId",
         |    "customer_name": "$custId",
         |    "risk_score": $score,
         |    "risk_level": "$level",
         |    "triggered_rules": $rules,
         |    "transaction_timestamp": "$tsStr",
         |    "amount": $amountStr,
         |    "currency": "$currency",
         |    "country": "$country",
         |    "status": "$status",
         |    "known_device": $knownDevice
         |  }""".stripMargin
    }
    val alertsJson = "[\n" + alertsJsonRows.mkString(",\n") + "\n]"

    // 5. Daily Timeline Export
    val dailyTimelineDf = scoredDf
      .withColumn("date", date_format(col("transaction_timestamp"), "yyyy-MM-dd"))
      .groupBy("date")
      .agg(
        count("transaction_id").as("total_tx"),
        sum(when($"risk_score" > 0, 1).otherwise(0)).as("alerts_count"),
        max("risk_score").as("max_score")
      )
      .orderBy("date")

    val dailyTimelineJsonRows = dailyTimelineDf.collect().map { row =>
      val date = Option(row.getAs[String]("date")).getOrElse("Unknown")
      val totalTx = row.getAs[Long]("total_tx")
      val alertsCount = row.getAs[Long]("alerts_count")
      val maxScore = Option(row.getAs[Int]("max_score")).getOrElse(0)
      s"""  {
         |    "date": "$date",
         |    "total_tx": $totalTx,
         |    "alerts_count": $alertsCount,
         |    "max_score": $maxScore
         |  }""".stripMargin
    }
    val dailyTimelineJson = "[\n" + dailyTimelineJsonRows.mkString(",\n") + "\n]"

    // 6. Manifest
    val manifestJson =
      s"""{
         |  "batch_id": "$batchId",
         |  "export_timestamp_utc": "$timestamp",
         |  "files": [
         |    "summary_kpis.json",
         |    "rule_statistics.json",
         |    "customer_risk_summary.json",
         |    "fraud_alerts.json",
         |    "daily_timeline.json"
         |  ]
         |}""".stripMargin

    // Write files to export target locations
    val targetDirs = Seq(
      Paths.get(config.dashboardExportDir),
      Paths.get(config.dashboardRootDataDir)
    )

    targetDirs.foreach { dir =>
      Files.createDirectories(dir)
      writeFile(dir.resolve("summary_kpis.json"), kpisJson)
      writeFile(dir.resolve("rule_statistics.json"), ruleStatsJson)
      writeFile(dir.resolve("customer_risk_summary.json"), customerSummaryJson)
      writeFile(dir.resolve("fraud_alerts.json"), alertsJson)
      writeFile(dir.resolve("daily_timeline.json"), dailyTimelineJson)
      writeFile(dir.resolve("export_manifest.json"), manifestJson)
    }

    println(s"=== Dashboard Data Export Complete for Batch: $batchId ===")
    println(s"Exported JSON files to: ${config.dashboardExportDir} and ${config.dashboardRootDataDir}")
  }

  private def writeFile(path: java.nio.file.Path, content: String): Unit = {
    Files.write(
      path,
      content.getBytes("UTF-8"),
      StandardOpenOption.CREATE,
      StandardOpenOption.TRUNCATE_EXISTING
    )
  }
}
