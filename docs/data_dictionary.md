# Data Dictionary — Fraud Pattern Detector

## 1. General conventions

- Encoding: UTF-8.
- CSV delimiter: comma.
- CSV headers: required.
- Decimal separator: period.
- Monetary currency: EUR only in V1.
- Timestamp format: ISO 8601 UTC (`yyyy-MM-dd'T'HH:mm:ss'Z'`).
- Internal timestamps: Spark `TimestampType` under UTC session configuration.
- Missing CSV values: empty field.
- Identifiers: strings, not numeric values.
- Column naming: `snake_case`.
- Money: Spark `DecimalType(18,2)`.
- Rule flags: Boolean.
- Scores: nonnegative integer.

No real personal or banking information may be generated.

## 2. Logical relationships

```text
customers (1)
     |
     +----------< transactions (N)
     |
     +----------< devices (N)

transactions.device_id
     |
     +-- matched using customer_id + device_id
         against the registered devices table
```

A device may be registered to one customer in V1.

A transaction can reference an unregistered device because unknown-device activity is a valid detection scenario.

## 3. Source dataset: customers.csv

Target: 1,000 rows.

| Column | Logical type | Required | Description |
|---|---|---|---|
| customer_id | string | Yes | Unique customer identifier |
| home_country | string | Yes | ISO 3166-1 alpha-2 country |
| risk_profile | string | Yes | LOW, MEDIUM or HIGH |
| created_at | timestamp | Yes | Synthetic customer creation time |

Example:

```csv
customer_id,home_country,risk_profile,created_at
C000001,ES,LOW,2025-01-01T10:00:00Z
C000002,FR,MEDIUM,2025-01-02T11:30:00Z
```

Rules:

- `customer_id` must be unique.
- `risk_profile` must be one of LOW, MEDIUM or HIGH.
- `home_country` must belong to the configured supported-country catalog.
- `created_at` must not occur after the batch's logical observation end.
- Transaction timestamps must not precede the referenced customer's creation timestamp.

## 4. Source dataset: devices.csv

Target: 1,500 rows.

| Column | Logical type | Required | Description |
|---|---|---|---|
| device_id | string | Yes | Unique registered device identifier |
| customer_id | string | Yes | Device owner |
| device_type | string | Yes | MOBILE, DESKTOP or TABLET |
| registered_at | timestamp | Yes | Registration timestamp |

Example:

```csv
device_id,customer_id,device_type,registered_at
D000001,C000001,MOBILE,2025-01-02T08:00:00Z
D000002,C000002,DESKTOP,2025-01-03T09:00:00Z
```

Rules:

- Device ID is unique.
- Device owner must exist in valid customers.
- Device type must be supported.
- Registration timestamp must be valid.
- A device is considered known at transaction time only when its registration timestamp is no later than the transaction timestamp.

This last condition avoids treating devices as known before registration.

## 5. Source dataset: transactions.csv

Target: 50,000 source rows, including deliberately injected defects.

| Column | Logical type | Required | Description |
|---|---|---|---|
| transaction_id | string | Yes | Transaction business identifier |
| customer_id | string | Yes | Customer performing transaction |
| device_id | string | Yes | Device used |
| transaction_timestamp | timestamp | Yes | Transaction UTC timestamp |
| amount | decimal(18,2) | Yes | Transaction amount in EUR |
| currency | string | Yes | EUR |
| merchant_id | string | Yes | Synthetic merchant identifier |
| merchant_category | string | Yes | Configured merchant category |
| country | string | Yes | Transaction country |
| status | string | Yes | APPROVED or DECLINED |

Example:

```csv
transaction_id,customer_id,device_id,transaction_timestamp,amount,currency,merchant_id,merchant_category,country,status
T000001,C000001,D000001,2026-01-10T09:00:00Z,45.90,EUR,M0001,RETAIL,ES,APPROVED
T000002,C000002,D009999,2026-01-10T09:05:00Z,120.00,EUR,M0002,ONLINE,FR,DECLINED
```

The second example represents an unknown device, not necessarily an invalid transaction.

## 6. Supported catalogs

### Transaction statuses

- APPROVED.
- DECLINED.

### Customer risk profiles

- LOW.
- MEDIUM.
- HIGH.

### Device types

- MOBILE.
- DESKTOP.
- TABLET.

### Merchant categories

- RETAIL.
- ONLINE.
- TRAVEL.
- FOOD.
- ENTERTAINMENT.
- SERVICES.

### Supported countries

Initial catalog:

- ES.
- FR.
- PT.
- DE.
- IT.
- GB.
- US.
- BO.

This catalog is intentionally limited for V1; it is not a complete list of valid countries.

All generated values must respect these catalogs except deliberately injected invalid rows.

## 7. Dataset generation modes

### Mode A — Controlled fixture

A small, manually inspectable dataset.

Purpose:

- Validate individual rules.
- Verify temporal boundaries.
- Explain outputs.
- Support ScalaTest integration tests.

### Mode B — Development

Recommended:

- 100 customers.
- 150 devices.
- 1,000 transactions.

Purpose:

- Implement processing stages.
- Debug schemas and joins.
- Validate local execution.

### Mode C — Full V1

- 1,000 customers.
- 1,500 devices.
- 50,000 transaction source rows.

Purpose:

- Complete pipeline demonstration.
- Rule analysis.
- Dashboard generation.
- Performance measurements.

## 8. Synthetic scenario manifest

File: `generator_manifest.json`.

Required metadata:

- `generator_version`
- `seed`
- `generation_mode`
- `generation_timestamp_utc`
- `observation_start_utc`
- `observation_end_utc`
- `customer_count`
- `device_count`
- `transaction_count`
- `injected_scenario_counts`
- `injected_quality_defect_counts`

A separate `scenario_expected.csv` contains:

| Column | Type | Description |
|---|---|---|
| transaction_id | string | Transaction ID |
| scenario_id | string | Injected scenario identifier |
| expected_r001 | boolean | Expected trigger for R001 |
| expected_r002 | boolean | Expected trigger for R002 |
| expected_r003 | boolean | Expected trigger for R003 |
| expected_r004 | boolean | Expected trigger for R004 |
| expected_r005 | boolean | Expected trigger for R005 |
| expected_risk_score | integer | Expected total risk score |
| expected_risk_level | string | Expected risk level (LOW, MEDIUM, HIGH, CRITICAL) |
| valid_for_rule_testing | boolean | Whether quality defects can interfere |

This manifest is a test fixture, not real fraud ground truth.

Scenarios may overlap intentionally. Scenario counts are not necessarily mutually exclusive and must not be added to calculate total transaction count.

For deterministic integration cases, the generator must provide a separate fixture with unambiguous expected outcomes.

## 9. Bronze datasets

Paths:

```text
data/bronze/<batch_id>/customers/
data/bronze/<batch_id>/devices/
data/bronze/<batch_id>/transactions/
```

Bronze preserves source fields and adds:

- `batch_id`: string.
- `source_file`: string.
- `source_row_number`: long or equivalent stable row reference.
- `ingested_at_utc`: timestamp.

Parsing and type-conversion validity must be recorded without silently dropping bad records.

## 10. Silver datasets

Paths:

```text
data/silver/<batch_id>/customers/
data/silver/<batch_id>/devices/
data/silver/<batch_id>/transactions/
data/silver/<batch_id>/enriched_transactions/
```

### Enriched transaction contract

Contains all validated transaction columns plus:

| Column | Type | Description |
|---|---|---|
| home_country | string | Customer home country |
| risk_profile | string | Customer risk profile |
| known_device | boolean | Registered device for customer at transaction time |
| batch_id | string | Processing batch |

Constraints:

- One row per unique valid transaction.
- No missing required customer attributes.
- Unknown devices remain eligible for rule evaluation.
- Enrichment must not multiply transaction rows.
- Silver contains no fraud classification or risk score.

## 11. Gold: scored_transactions

Path:

```text
data/gold/<batch_id>/scored_transactions/
```

Columns:

| Column | Type | Description |
|---|---|---|
| batch_id | string | Processing batch |
| transaction_id | string | Unique transaction |
| customer_id | string | Customer |
| transaction_timestamp | timestamp | UTC |
| amount | decimal(18,2) | Amount |
| status | string | Transaction status |
| country | string | Country |
| risk_profile | string | Customer profile |
| known_device | boolean | Device registration result |
| rule_r001 | boolean | Burst indicator |
| rule_r002 | boolean | High amount |
| rule_r003 | boolean | Rapid country change |
| rule_r004 | boolean | Declined transactions |
| rule_r005 | boolean | Unknown device combination |
| risk_score | integer | Total rule score |
| risk_level | string | LOW, MEDIUM, HIGH, CRITICAL |
| triggered_rules | array of strings | Triggered rule identifiers |

All valid Silver transactions appear in `scored_transactions`.

## 12. Gold: fraud_alerts

Path:

```text
data/gold/<batch_id>/fraud_alerts/
```

Contains the scored transaction fields for rows with at least one triggered rule.

Additional field:

- `alert_id`: deterministic identifier derived from batch ID and transaction ID.

No alert is generated when all rules are false.

One transaction generates at most one aggregated alert, even when several rules trigger.

## 13. Gold: customer_risk_summary

| Column | Description |
|---|---|
| batch_id | Batch |
| customer_id | Customer |
| total_transactions | Valid analyzed transactions |
| alert_count | Transactions with at least one rule |
| average_risk_score | Mean score, including zero |
| max_risk_score | Maximum transaction score |
| high_risk_count | HIGH and CRITICAL transactions |
| critical_risk_count | CRITICAL transactions |

## 14. Gold: rule_statistics

| Column | Description |
|---|---|
| batch_id | Batch |
| rule_id | R001–R005 |
| trigger_count | Number of transactions where rule is true |
| trigger_rate | Count divided by analyzed valid transactions |
| configured_score | Rule score |

Rule trigger counts may overlap and their sum may exceed the number of alerts.

## 15. Gold: daily_statistics

Date grouping uses UTC.

| Column | Description |
|---|---|
| batch_id | Batch |
| transaction_date | UTC date |
| total_transactions | Valid analyzed transactions |
| total_amount_eur | Sum of valid transaction amounts |
| alert_count | Transactions with at least one rule |
| high_risk_count | HIGH and CRITICAL |
| critical_risk_count | CRITICAL |
| average_risk_score | Mean transaction score |

## 16. Rejection and quality reports

Rejected transaction records must retain:

- Batch ID.
- Dataset name.
- Stable source-row reference.
- Original source representation.
- Error-code collection.
- Primary rejection reason.

Metrics must distinguish:

- Physical source rows.
- Parsed rows.
- Rejected rows.
- Deduplicated rows.
- Final valid unique rows.

## 17. Contract invariants

The following must always hold:

1. Every scored transaction maps to one valid Silver transaction.
2. Every alert maps to one scored transaction.
3. Every alerted transaction has at least one true rule.
4. No two Gold alerts share the same batch ID and transaction ID.
5. Every rule flag is non-null.
6. Every score is nonnegative.
7. Every risk level matches the score bands.
8. Rule statistics match the underlying scored rows.
9. Customer summaries match scored rows.
10. Dashboard totals match Gold exports.
