package frauddetector.config

case class AppConfig(
  batchId: String = "batch_sample_01",
  rawInputDir: String = "../data/sample",
  bronzeOutputDir: String = "../data/bronze",
  silverOutputDir: String = "../data/silver",
  quarantineOutputDir: String = "../data/quarantine"
) {
  def customersRawPath: String = s"$rawInputDir/customers.csv"
  def devicesRawPath: String = s"$rawInputDir/devices.csv"
  def transactionsRawPath: String = s"$rawInputDir/transactions.csv"

  def customersBronzePath: String = s"$bronzeOutputDir/$batchId/customers"
  def devicesBronzePath: String = s"$bronzeOutputDir/$batchId/devices"
  def transactionsBronzePath: String = s"$bronzeOutputDir/$batchId/transactions"

  def customersSilverPath: String = s"$silverOutputDir/$batchId/customers"
  def devicesSilverPath: String = s"$silverOutputDir/$batchId/devices"
  def transactionsSilverPath: String = s"$silverOutputDir/$batchId/transactions"
  def enrichedTransactionsSilverPath: String = s"$silverOutputDir/$batchId/enriched_transactions"

  def quarantinePath: String = s"$quarantineOutputDir/$batchId/rejections"

  def goldOutputDir: String = "../data/gold"
  def scoredTransactionsGoldPath: String = s"$goldOutputDir/$batchId/scored_transactions"
  def fraudAlertsGoldPath: String = s"$goldOutputDir/$batchId/fraud_alerts"

  def dashboardExportDir: String = s"$goldOutputDir/$batchId/dashboard_exports"
  def dashboardRootDataDir: String = "../dashboard/data"
}

object AppConfig {
  def defaultConfig(): AppConfig = {
    val rawFile = new java.io.File("../data/raw/transactions.csv")
    if (rawFile.exists()) {
      AppConfig(batchId = "batch_full_01", rawInputDir = "../data/raw")
    } else {
      AppConfig(batchId = "batch_sample_01", rawInputDir = "../data/sample")
    }
  }
}
