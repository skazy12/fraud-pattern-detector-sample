# Data Quality Specification — Fraud Pattern Detector

## 1. Objective

Ensure the detection engine receives structurally valid, internally consistent and uniquely identifiable transactions.

Data quality and fraud detection are separate concerns.

A suspicious transaction can be perfectly valid.

An invalid transaction must not enter the detection engine.

## 2. Validation stages

### Stage A — Structural validation

Verify:

- Required files exist.
- Expected headers are present.
- Encoding and delimiter are supported.
- Records can be parsed.
- Required column types can be converted.
- Extra or missing columns follow the declared strict-schema policy.

### Stage B — Field validation

Verify:

- Mandatory values are present.
- Identifiers have valid formats.
- Amounts are positive.
- Dates are parsable.
- Enumeration fields use allowed values.
- Country codes are supported.

### Stage C — Referential validation

Verify:

- Each transaction references a valid customer.
- Each registered device references a valid customer.

An unknown transaction device does not cause rejection.

### Stage D — Business validation

Verify:

- Transaction timestamps lie inside the batch observation period.
- Transactions do not precede the customer's creation timestamp.
- Registered-device dates are valid.
- Only EUR transactions are accepted.
- Monetary values fit the declared decimal type.

### Stage E — Deduplication

Identify repeated transaction business identifiers.

## 3. Quality rule catalog

| Code | Condition | Action |
|---|---|---|
| Q001 | Missing transaction ID | Reject |
| Q002 | Missing customer ID | Reject |
| Q003 | Invalid transaction timestamp | Reject |
| Q004 | Amount missing, zero or negative | Reject |
| Q005 | Unsupported currency | Reject |
| Q006 | Unsupported country | Reject |
| Q007 | Invalid transaction status | Reject |
| Q008 | Customer reference missing | Reject |
| Q009A | Exact duplicate transaction ID | Deduplicate (keep canonical, remove copies) |
| Q009B | Conflicting duplicate transaction ID | Quarantine (exclude all copies) |
| Q010 | Missing device ID | Reject |
| Q011 | Invalid merchant information | Reject |
| Q012 | Transaction outside observation period | Reject |
| Q013 | Transaction before customer creation | Reject |
| Q014 | Unsupported customer risk profile | Reject customer; reject dependent transactions |
| Q015 | Invalid registered-device record | Reject device registration record |
| Q016 | Malformed source CSV record | Reject and preserve source details |

Malformed customer or device records must also be tracked using dataset-specific quality reports.

## 4. Duplicate policy

Business uniqueness is defined by `transaction_id` within a batch.

### Exact duplicate

Two source rows contain the same transaction ID and identical business fields.

Action:

- Keep one deterministic canonical record.
- Count and report additional copies.
- Preserve duplicate source-row references.

### Conflicting duplicate

Two rows use the same transaction ID but have different business-field values.

Action:

- Quarantine all conflicting rows for that transaction ID.
- Do not arbitrarily choose one.
- Exclude that transaction ID from Silver.
- Count the excluded rows.

### Deterministic selection

For exact duplicates, keep the row with the smallest stable source-row reference.

Spark execution order or input partition order must not determine the surviving row.

## 5. Customer validation

Customers must have:

- Unique customer ID.
- Valid home country.
- Valid risk profile.
- Valid creation timestamp.

Conflicting duplicate customer IDs are quarantined.

Transactions referencing excluded customers must be rejected.

## 6. Registered-device validation

Registered devices must have:

- Valid unique device ID.
- Existing valid customer owner.
- Supported device type.
- Valid registration timestamp.

Conflicting device ownership is rejected.

A malformed registered-device record must not be silently converted into a known-device relationship.

## 7. Unknown device handling

An unknown transaction device may be:

- Absent from the device registry.
- Registered to a different customer.
- Registered after the transaction time.

All three produce:

`known_device = false`

These cases are legitimate analytical inputs for R005.

Unknown-device activity is not rejected solely because the registration is missing.

## 8. Normalization policy

Allowed normalizations:

- Trim surrounding whitespace from string fields.
- Convert enumerations and country codes to uppercase.
- Parse timestamps into UTC.
- Convert amounts to a consistent decimal type.

Forbidden silent corrections:

- Replace negative amounts with positive values.
- Invent missing customer IDs.
- Replace unsupported countries with ES.
- Change DECLINED to APPROVED.
- Substitute arbitrary transaction dates.
- Invent a device registration.

All transformations must be documented and testable.

## 9. Rejection data contract

Every rejected row should retain:

- `batch_id`
- `dataset_name`
- `source_file`
- `source_row_reference`
- `raw_record`
- `quality_error_codes`
- `primary_error_code`

Rows may have multiple quality error codes.

The primary code is chosen deterministically using the documented quality-rule ordering.

Rejections must be inspectable after pipeline execution.

## 10. Data quality metrics

Report at least:

- Source row count.
- Parsing rejection count.
- Semantic rejection count.
- Exact duplicate count.
- Conflicting duplicate count.
- Valid unique row count.
- Rejection count by quality code.
- Unknown-device count.
- Referential failure count.

Unknown-device count is an analytical metric, not a rejection count.

## 11. Count reconciliation

For each dataset, reconcile physical input rows with final accepted and excluded rows.

For transactions, the report must account for:

```text
source rows
=
accepted unique rows
+ exact duplicate copies removed
+ rejected or quarantined source rows
```

Classification groups must be disjoint for this reconciliation.

A rejected row may carry several error codes but counts only once in the rejected-row total.

## 12. Failure policy

Stop the whole batch when:

- A mandatory file is absent.
- Required columns are missing.
- Configuration is invalid.
- The source file is unreadable.
- A required reference dataset cannot be produced.
- An internal processing error occurs.
- Gold output verification fails.

Individual invalid business records normally go to quarantine instead of stopping the entire run.

## 13. Quality acceptance criteria

- No duplicate transaction IDs in Silver.
- No invalid mandatory values in Silver.
- No orphan transaction-to-customer references.
- Rejected records are traceable.
- Duplicate selection is deterministic.
- Unknown devices remain analyzable.
- Processing counts reconcile.
- All defined quality tests pass.

## 14. Reporting limitations

A low rejection rate does not prove the dataset is realistic.

Likewise, zero rejected rows does not imply zero suspicious transactions.

Quality metrics and risk indicators must be presented separately.
