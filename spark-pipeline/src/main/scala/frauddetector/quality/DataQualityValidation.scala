package frauddetector.quality

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.DecimalType
import frauddetector.config.AppConfig
import frauddetector.quality.QualityRules._

case class QualityMetrics(
  sourceRowCount: Long,
  validUniqueRowCount: Long,
  exactDuplicateCount: Long,
  conflictingDuplicateCount: Long,
  parsingRejectionCount: Long,
  semanticRejectionCount: Long,
  unknownDeviceCount: Long,
  referentialFailureCount: Long
)

case class ValidationResult(
  validCustomers: DataFrame,
  validDevices: DataFrame,
  validTransactions: DataFrame,
  enrichedTransactions: DataFrame,
  quarantine: DataFrame,
  metrics: QualityMetrics
)

class DataQualityValidation(spark: SparkSession, config: AppConfig) {
  import spark.implicits._

  def run(customersBronze: DataFrame, devicesBronze: DataFrame, transactionsBronze: DataFrame): ValidationResult = {
    println(s"=== Running Data Quality Validation for Batch: ${config.batchId} ===")

    // 1. Validate Customers
    val validCountries = spark.createDataset(SupportedCountries.toSeq).toDF("valid_country")
    val validRiskProfiles = spark.createDataset(SupportedRiskProfiles.toSeq).toDF("valid_risk_profile")

    val customerValidated = customersBronze
      .withColumn("q_id_missing", col("customer_id").isNull || length(trim(col("customer_id"))) === 0)
      .withColumn("q_country_invalid", col("home_country").isNull || !upper(trim(col("home_country"))).isin(SupportedCountries.toSeq: _*))
      .withColumn("q_risk_invalid", col("risk_profile").isNull || !upper(trim(col("risk_profile"))).isin(SupportedRiskProfiles.toSeq: _*))
      .withColumn("q_created_missing", col("created_at").isNull)

    val validCustomers = customerValidated
      .filter(!$"q_id_missing" && !$"q_country_invalid" && !$"q_risk_invalid" && !$"q_created_missing")
      .select("customer_id", "home_country", "risk_profile", "created_at", "batch_id")
      .dropDuplicates("customer_id")

    // 2. Validate Devices
    val validDevices = devicesBronze
      .withColumn("q_dev_id_missing", col("device_id").isNull || length(trim(col("device_id"))) === 0)
      .withColumn("q_cust_id_missing", col("customer_id").isNull || length(trim(col("customer_id"))) === 0)
      .withColumn("q_type_invalid", col("device_type").isNull || !upper(trim(col("device_type"))).isin(SupportedDeviceTypes.toSeq: _*))
      .withColumn("q_reg_missing", col("registered_at").isNull)
      .filter(!$"q_dev_id_missing" && !$"q_cust_id_missing" && !$"q_type_invalid" && !$"q_reg_missing")
      .join(validCustomers.select("customer_id").withColumnRenamed("customer_id", "valid_cust_id"), $"customer_id" === $"valid_cust_id", "inner")
      .select("device_id", "customer_id", "device_type", "registered_at", "batch_id")
      .dropDuplicates("device_id")

    // 3. Validate Transactions
    val sourceTxCount = transactionsBronze.count()

    // Step 3a: Field validations
    val txFieldValidated = transactionsBronze
      .withColumn("parsed_amount", col("amount").cast(DecimalType(18, 2)))
      .withColumn("parsed_timestamp", to_timestamp(col("transaction_timestamp")))
      .withColumn("q001", col("transaction_id").isNull || length(trim(col("transaction_id"))) === 0)
      .withColumn("q002", col("customer_id").isNull || length(trim(col("customer_id"))) === 0)
      .withColumn("q003", $"parsed_timestamp".isNull)
      .withColumn("q004", $"parsed_amount".isNull || $"parsed_amount" <= 0)
      .withColumn("q005", col("currency").isNull || upper(trim(col("currency"))) =!= "EUR")
      .withColumn("q006", col("country").isNull || !upper(trim(col("country"))).isin(SupportedCountries.toSeq: _*))
      .withColumn("q007", col("status").isNull || !upper(trim(col("status"))).isin(SupportedStatuses.toSeq: _*))
      .withColumn("q010", col("device_id").isNull || length(trim(col("device_id"))) === 0)
      .withColumn("q011", col("merchant_id").isNull || length(trim(col("merchant_id"))) === 0 || col("merchant_category").isNull || !upper(trim(col("merchant_category"))).isin(SupportedMerchantCategories.toSeq: _*))

    val hasFieldErrors = $"q001" || $"q002" || $"q003" || $"q004" || $"q005" || $"q006" || $"q007" || $"q010" || $"q011"

    val fieldFailures = txFieldValidated.filter(hasFieldErrors)
      .withColumn("dataset_name", lit("transactions"))
      .withColumn("source_row_reference", col("source_row_number"))
      .withColumn("raw_record", concat_ws(",", col("transaction_id"), col("customer_id"), col("device_id"), col("transaction_timestamp"), col("amount"), col("currency"), col("merchant_id"), col("merchant_category"), col("country"), col("status")))
      .withColumn("primary_error_code",
        when($"q001", lit(Q001_MISSING_TRANSACTION_ID))
          .when($"q002", lit(Q002_MISSING_CUSTOMER_ID))
          .when($"q003", lit(Q003_INVALID_TRANSACTION_TIMESTAMP))
          .when($"q004", lit(Q004_INVALID_AMOUNT))
          .when($"q005", lit(Q005_UNSUPPORTED_CURRENCY))
          .when($"q006", lit(Q006_UNSUPPORTED_COUNTRY))
          .when($"q007", lit(Q007_INVALID_STATUS))
          .when($"q010", lit(Q010_MISSING_DEVICE_ID))
          .otherwise(lit(Q011_INVALID_MERCHANT_INFO))
      )
      .withColumn("quality_error_codes", array($"primary_error_code"))
      .select("batch_id", "dataset_name", "source_file", "source_row_reference", "raw_record", "quality_error_codes", "primary_error_code")

    val fieldPass = txFieldValidated.filter(!hasFieldErrors)

    // Step 3b: Customer referential check (Q008) and creation date check (Q013)
    val joinedWithCustomer = fieldPass
      .join(validCustomers.select($"customer_id".as("cust_ref"), $"created_at".as("cust_created_at"), $"home_country".as("cust_home_country"), $"risk_profile".as("cust_risk_profile")), $"customer_id" === $"cust_ref", "left")
      .withColumn("q008", $"cust_ref".isNull)
      .withColumn("q013", $"cust_created_at".isNotNull && $"parsed_timestamp" < to_timestamp($"cust_created_at"))

    val referentialFailures = joinedWithCustomer.filter($"q008" || $"q013")
      .withColumn("dataset_name", lit("transactions"))
      .withColumn("source_row_reference", col("source_row_number"))
      .withColumn("raw_record", concat_ws(",", col("transaction_id"), col("customer_id"), col("device_id"), col("transaction_timestamp"), col("amount"), col("currency"), col("merchant_id"), col("merchant_category"), col("country"), col("status")))
      .withColumn("primary_error_code", when($"q008", lit(Q008_CUSTOMER_NOT_FOUND)).otherwise(lit(Q013_TRANSACTION_BEFORE_CUSTOMER)))
      .withColumn("quality_error_codes", array($"primary_error_code"))
      .select("batch_id", "dataset_name", "source_file", "source_row_reference", "raw_record", "quality_error_codes", "primary_error_code")

    val referentialPass = joinedWithCustomer.filter(!$"q008" && !$"q013")

    // Step 3c: Deduplication (Q009A vs Q009B)
    val businessHashCol = md5(concat_ws("||",
      upper(trim(col("customer_id"))),
      upper(trim(col("device_id"))),
      col("parsed_timestamp"),
      col("parsed_amount"),
      upper(trim(col("currency"))),
      upper(trim(col("merchant_id"))),
      upper(trim(col("merchant_category"))),
      upper(trim(col("country"))),
      upper(trim(col("status")))
    ))

    val withHash = referentialPass.withColumn("business_hash", businessHashCol)

    val windowTxId = Window.partitionBy("transaction_id")

    val groupedDuplicates = withHash
      .withColumn("tx_row_count", count("transaction_id").over(windowTxId))
      .withColumn("distinct_hash_count", size(collect_set("business_hash").over(windowTxId)))

    // Conflicting duplicates (Q009B) -> distinct_hash_count > 1
    val conflictingFailures = groupedDuplicates.filter($"distinct_hash_count" > 1)
      .withColumn("dataset_name", lit("transactions"))
      .withColumn("source_row_reference", col("source_row_number"))
      .withColumn("raw_record", concat_ws(",", col("transaction_id"), col("customer_id"), col("device_id"), col("transaction_timestamp"), col("amount"), col("currency"), col("merchant_id"), col("merchant_category"), col("country"), col("status")))
      .withColumn("primary_error_code", lit(Q009B_CONFLICTING_DUPLICATE))
      .withColumn("quality_error_codes", array(lit(Q009B_CONFLICTING_DUPLICATE)))
      .select("batch_id", "dataset_name", "source_file", "source_row_reference", "raw_record", "quality_error_codes", "primary_error_code")

    val nonConflicting = groupedDuplicates.filter($"distinct_hash_count" === 1)

    // Exact duplicates (Q009A) -> keep rank == 1 by source_row_number
    val windowRank = Window.partitionBy("transaction_id").orderBy(col("source_row_number").asc)

    val rankedExact = nonConflicting.withColumn("dup_rank", row_number().over(windowRank))

    val survivingTransactions = rankedExact.filter($"dup_rank" === 1)
    val exactDuplicatesRemoved = rankedExact.filter($"dup_rank" > 1)

    val exactDuplicateCount = exactDuplicatesRemoved.count()
    val conflictingDuplicateCount = conflictingFailures.count()

    // Step 3d: Device matching -> known_device boolean
    val joinedWithDevice = survivingTransactions
      .join(
        validDevices.select($"device_id".as("reg_dev_id"), $"customer_id".as("reg_cust_id"), $"registered_at".as("reg_at")),
        $"device_id" === $"reg_dev_id" && $"customer_id" === $"reg_cust_id" && to_timestamp($"reg_at") <= $"parsed_timestamp",
        "left"
      )
      .withColumn("known_device", $"reg_dev_id".isNotNull)

    val validTransactions = survivingTransactions
      .select(
        col("transaction_id"),
        col("customer_id"),
        col("device_id"),
        col("parsed_timestamp").as("transaction_timestamp"),
        col("parsed_amount").as("amount"),
        col("currency"),
        col("merchant_id"),
        col("merchant_category"),
        col("country"),
        col("status"),
        col("batch_id")
      )

    val enrichedTransactions = joinedWithDevice
      .select(
        col("transaction_id"),
        col("customer_id"),
        col("device_id"),
        col("parsed_timestamp").as("transaction_timestamp"),
        col("parsed_amount").as("amount"),
        col("currency"),
        col("merchant_id"),
        col("merchant_category"),
        col("country"),
        col("status"),
        col("cust_home_country").as("home_country"),
        col("cust_risk_profile").as("risk_profile"),
        col("known_device"),
        col("batch_id")
      )

    val allQuarantine = fieldFailures.union(referentialFailures).union(conflictingFailures)

    val validUniqueRowCount = enrichedTransactions.count()
    val semanticRejectionCount = fieldFailures.count() + referentialFailures.count()
    val unknownDeviceCount = enrichedTransactions.filter(!$"known_device").count()
    val referentialFailureCount = referentialFailures.count()

    val metrics = QualityMetrics(
      sourceRowCount = sourceTxCount,
      validUniqueRowCount = validUniqueRowCount,
      exactDuplicateCount = exactDuplicateCount,
      conflictingDuplicateCount = conflictingDuplicateCount,
      parsingRejectionCount = 0L,
      semanticRejectionCount = semanticRejectionCount,
      unknownDeviceCount = unknownDeviceCount,
      referentialFailureCount = referentialFailureCount
    )

    println(s"Quality Metrics: Source=$sourceTxCount | ValidUnique=$validUniqueRowCount | ExactDupesRemoved=$exactDuplicateCount | ConflictingDupesQuarantined=$conflictingDuplicateCount | Rejections=$semanticRejectionCount | UnknownDevices=$unknownDeviceCount")

    ValidationResult(
      validCustomers = validCustomers,
      validDevices = validDevices,
      validTransactions = validTransactions,
      enrichedTransactions = enrichedTransactions,
      quarantine = allQuarantine,
      metrics = metrics
    )
  }
}
