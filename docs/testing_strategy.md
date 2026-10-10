# Testing Strategy — Fraud Pattern Detector

## 1. Testing objectives

Verify that the pipeline:

- Accepts valid records.
- Rejects invalid records correctly.
- Preserves valid transaction identities.
- Performs deterministic deduplication.
- Applies the five detection rules according to specification.
- Calculates correct scores.
- Produces consistent analytical datasets.
- Supports reproducible reruns.

## 2. Test levels

### Unit tests

Test isolated logic where possible:

- Configuration validation.
- Risk score mapping.
- Severity boundaries.
- Rule condition helpers.
- Deterministic identifier generation.

### Spark transformation tests

Use small in-memory Spark DataFrames to test:

- Validation transformations.
- Deduplication.
- Joins.
- Temporal windows.
- Rule activation.
- Aggregations.

### Integration tests

Execute several pipeline components together using small fixtures.

### End-to-end tests

Run the complete pipeline on a controlled fixture and compare outputs against expected results.

### Smoke tests

Confirm that the full generated dataset processes successfully and produces required outputs.

## 3. Controlled fixture

Maintain a small fixture in:

`data/sample/`

It must contain:

- Customers with different risk profiles.
- Registered and unregistered devices.
- Normal transactions.
- One isolated scenario for each rule.
- Several combined-rule scenarios.
- Valid boundary cases.
- Selected invalid records.

Do not use only random data to validate correctness.

## 4. Rule test matrix

| Test ID | Scenario | Expected result |
|---|---|---|
| R001-T01 | 5 approvals in 8 min | True |
| R001-T02 | 4 approvals in 10 min | False |
| R001-T03 | 5 approvals spanning exactly 10 min | True |
| R001-T04 | 4 approvals and 1 decline | False |
| R001-T05 | 5 simultaneous approvals | True for all simultaneous events |
| R002-T01 | LOW profile, 2,001 EUR | True |
| R002-T02 | LOW profile, 2,000 EUR | False |
| R002-T03 | HIGH profile, 1,001 EUR | True |
| R002-T04 | Declined amount above threshold | False |
| R003-T01 | ES to FR within 35 min | True on second event |
| R003-T02 | ES to FR after 61 min | False |
| R003-T03 | ES to ES within 10 min | False |
| R003-T04 | Different countries exactly 60 min apart | True |
| R003-T05 | Deterministic same-timestamp ordering | Matches transaction ID ordering |
| R004-T01 | 3 declines in 9 min | True on third event |
| R004-T02 | 2 declines in 15 min | False |
| R004-T03 | 3 declines exactly 15 min apart | True |
| R005-T01 | Unknown device plus R002 | True |
| R005-T02 | Unknown device only | False |
| R005-T03 | Known device plus R002 | False |
| R005-T04 | Device registered after event plus R002 | True |

Each test must use controlled input, expected flags and meaningful assertions.

## 5. Scoring tests

Verify:

- No rule → 0, LOW.
- R004 → 15, LOW.
- R001 → 20, MEDIUM.
- R002 → 30, MEDIUM.
- R003 → 40, HIGH.
- R001 + R003 → 60, CRITICAL.
- R001 + R002 + R005 → 60, CRITICAL.

Also test score-band boundaries directly:

- 19 / 20.
- 39 / 40.
- 59 / 60.

Boundary tests may use synthetic score values independent of the default rule combination scores.

## 6. Data quality tests

Required cases:

1. Missing transaction ID.
2. Missing customer.
3. Negative amount.
4. Zero amount.
5. Malformed timestamp.
6. Unsupported country.
7. Unsupported status.
8. Exact duplicate.
9. Conflicting duplicate.
10. Orphan customer reference.
11. Unknown device accepted.
12. Invalid device registration.
13. Invalid customer risk profile.
14. Several errors on one row.
15. Amount exceeding supported decimal range.
16. Valid timestamp at observation boundary.

## 7. Determinism tests

Generate the same dataset twice with identical parameters.

Compare:

- Business records.
- Scenario assignments.
- Expected rule fixture results.
- Output record counts.

Generation timestamps and physical Parquet bytes may differ.

Determinism refers to meaningful data contents, not identical binary artifacts.

## 8. Integration invariants

Required assertions:

```text
count(scored_transactions)
=
count(valid_unique_silver_transactions)
```

```text
count(fraud_alerts)
=
count(scored_transactions where any rule is true)
```

```text
sum(rule_r001)
=
rule_statistics[R001].trigger_count
```

Apply equivalent checks for R002–R005.

Also verify that daily and customer aggregates reconcile with the underlying scored transactions.

## 9. Idempotency tests

Run the same batch twice.

Expected:

- Identical business-level Gold results.
- No accumulated duplicates.
- Stable alert identifiers.
- No mixing with unrelated batch outputs.
- Successful batch publication state.

## 10. Pipeline failure tests

Test:

- Missing input file.
- Invalid configuration.
- Missing mandatory column.
- Invalid schema definition.
- Unwritable output path.
- Failed intermediate stage.

The process must not report successful completion.

## 11. Test isolation

- Tests use temporary output directories.
- Tests must not modify production-style example outputs.
- Tests must not depend on previous runs.
- Spark sessions are created and stopped correctly.
- Fixtures are small and version controlled.
- Tests do not require internet access.

## 12. Test organization

Recommended:

```text
src/test/scala/frauddetector/
├── ingestion/
├── quality/
├── transformation/
├── rules/
├── scoring/
├── output/
└── integration/
```

## 13. Test execution

Standard command:

```bash
sbt test
```

The project README must document any additional integration or smoke-test commands introduced during implementation.

## 14. Performance evaluation

Measure full-pipeline duration using:

- Controlled fixture.
- 1,000 transactions.
- 50,000 transactions.
- Optional 500,000 transactions.

Record:

- Dataset size.
- Runtime environment.
- Spark configuration.
- Duration.
- Output size.
- Partition counts.
- Significant shuffle operations.

Tests must distinguish performance benchmarks from correctness tests.

## 15. Acceptance standard

V1.0 cannot be considered complete while any required automated test is failing.

All five rules need positive, negative and boundary coverage.

The full batch must complete successfully.

Aggregate reconciliation tests must pass.

Any intentionally unsupported case must be documented rather than silently ignored.
