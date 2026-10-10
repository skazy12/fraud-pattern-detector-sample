package frauddetector.quality

object QualityRules {
  // Quality Error Codes
  val Q001_MISSING_TRANSACTION_ID = "Q001"
  val Q002_MISSING_CUSTOMER_ID = "Q002"
  val Q003_INVALID_TRANSACTION_TIMESTAMP = "Q003"
  val Q004_INVALID_AMOUNT = "Q004"
  val Q005_UNSUPPORTED_CURRENCY = "Q005"
  val Q006_UNSUPPORTED_COUNTRY = "Q006"
  val Q007_INVALID_STATUS = "Q007"
  val Q008_CUSTOMER_NOT_FOUND = "Q008"
  val Q009A_EXACT_DUPLICATE = "Q009A"
  val Q009B_CONFLICTING_DUPLICATE = "Q009B"
  val Q010_MISSING_DEVICE_ID = "Q010"
  val Q011_INVALID_MERCHANT_INFO = "Q011"
  val Q012_OUTSIDE_OBSERVATION_PERIOD = "Q012"
  val Q013_TRANSACTION_BEFORE_CUSTOMER = "Q013"
  val Q014_UNSUPPORTED_RISK_PROFILE = "Q014"
  val Q015_INVALID_REGISTERED_DEVICE = "Q015"
  val Q016_MALFORMED_RECORD = "Q016"

  // Supported Catalogs
  val SupportedCountries: Set[String] = Set("ES", "FR", "PT", "DE", "IT", "GB", "US", "BO")
  val SupportedRiskProfiles: Set[String] = Set("LOW", "MEDIUM", "HIGH")
  val SupportedDeviceTypes: Set[String] = Set("MOBILE", "DESKTOP", "TABLET")
  val SupportedMerchantCategories: Set[String] = Set("RETAIL", "ONLINE", "TRAVEL", "FOOD", "ENTERTAINMENT", "SERVICES")
  val SupportedStatuses: Set[String] = Set("APPROVED", "DECLINED")
  val SupportedCurrencies: Set[String] = Set("EUR")
}
