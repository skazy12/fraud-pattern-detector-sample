# System Architecture — Fraud Pattern Detector

## 1. Architectural style

Fraud Pattern Detector follows a modular, batch-oriented data pipeline architecture.

It uses a simplified Medallion architecture:

- Bronze: ingested source records.
- Silver: validated and enriched records.
- Gold: risk assessment and analytical datasets.

## 2. End-to-end flow

```text
Python Synthetic Generator
          |
          v
data/raw/*.csv
          |
          v
Spark Ingestion
          |
          v
BRONZE
          |
          v
Data Quality Validation
     |           |
     v           v
  Rejected     Valid
   Data         Data
                 |
                 v
        Deduplication
                 |
                 v
              SILVER
                 |
                 v
       Customer + Device
          Enrichment
                 |
                 v
         Fraud Rules
           R001-R005
                 |
                 v
          Risk Scoring
                 |
                 v
               GOLD
                 |
          +------+------+
          |             |
          v             v
       Parquet     Dashboard Exports
                        |
                        v
                  HTML + Plotly
```

## 3. Component responsibilities

### 3.1 Synthetic generator

Language: Python.

Responsibilities:

- Generate customers, registered devices and transactions.
- Support configurable dataset sizes.
- Support a fixed random seed.
- Inject documented behavioral scenarios.
- Inject controlled data-quality defects.
- Export a scenario manifest for testing.
- Produce a generation report.

The generator does not execute fraud detection.

### 3.2 Ingestion module

Language: Scala.

Responsibilities:

- Create SparkSession.
- Read configuration.
- Load CSV datasets.
- Apply explicitly declared raw input schemas.
- Add ingestion metadata.
- Persist Bronze outputs.
- Report ingestion errors.

The ingestion module does not classify suspicious behavior.

### 3.3 Quality module

Responsibilities:

- Validate structural and semantic constraints.
- Normalize valid records.
- Identify duplicate business identifiers.
- Validate references.
- Separate invalid rows.
- Produce quality metrics.

Validation must be deterministic.

### 3.4 Silver preparation

Responsibilities:

- Persist cleaned customers.
- Persist cleaned device ownership records.
- Persist cleaned transactions.
- Enrich transactions with customer information.
- Determine whether the transaction device is registered to the customer.

The enrichment step must preserve one row per valid unique transaction.

### 3.5 Detection engine

Responsibilities:

- Apply the five detection rules.
- Preserve transaction identifiers.
- Produce one Boolean result per rule.
- Preserve rule-level explanatory attributes where useful.

Rules may calculate intermediate window features but must not independently write Gold outputs.

### 3.6 Risk scoring

Responsibilities:

- Map activated rules to configured point values.
- Calculate total scores.
- Assign risk levels.
- Produce ordered triggered-rule identifiers.
- Generate clear explanations.

The scoring module must not modify which rules fired.

### 3.7 Gold publishing

Responsibilities:

- Persist scored transactions.
- Publish alerts.
- Produce customer-level risk summaries.
- Produce rule statistics.
- Produce daily summaries.
- Publish quality and processing metrics.

### 3.8 Dashboard export

Responsibilities:

- Convert selected Gold results to dashboard-compatible JSON or CSV.
- Publish a generated dashboard data manifest.
- Avoid requiring a running Spark session to browse generated results.

### 3.9 Dashboard

Technologies: HTML, CSS, JavaScript and Plotly.

Responsibilities:

- Display KPIs and aggregated charts.
- Filter alert records.
- Explain rule IDs and severity levels.
- Show dataset and processing metadata.

It must not recalculate fraud rules.

## 4. Logical processing stages

| Stage | Input | Output |
|---|---|---|
| Generate | Seed and generator config | Source CSV + manifests |
| Ingest | Source CSV | Bronze Parquet |
| Validate | Bronze | Valid and rejected records |
| Prepare | Valid records | Silver Parquet |
| Enrich | Silver datasets | Enriched transactions |
| Detect | Enriched transactions | Rule flags |
| Score | Rule flags | Scored transactions |
| Publish | Scored transactions | Gold Parquet |
| Export | Gold | Dashboard JSON/CSV |
| Visualize | Dashboard exports | Static dashboard |

## 5. Repository structure

```text
fraud-pattern-detector/
├── docs/                        # Specifications and architectural documentation
├── spark-pipeline/              # Scala 3 + Apache Spark batch processing project
│   ├── build.sbt                # sbt configuration
│   ├── project/
│   │   └── build.properties     # sbt version (2.0.9)
│   ├── .jvmopts                 # Java 17 JVM options for Spark
│   ├── src/
│   │   ├── main/scala/frauddetector/
│   │   └── test/scala/frauddetector/
├── generator/                   # Python synthetic data generator project
│   ├── requirements.txt         # Python dependencies
│   ├── generate.py              # CLI entrypoint
│   ├── src/                     # Data generation modules
│   └── tests/                   # Python tests
├── data/                        # Datasets (raw, bronze, silver, gold, sample)
│   ├── sample/
│   ├── raw/
│   ├── bronze/
│   ├── silver/
│   ├── rejected/
│   └── gold/
├── dashboard/                   # Static interactive web dashboard
├── scripts/                     # Helper utilities
├── README.md                    # Main documentation index
```

Directories are created incrementally as required by the roadmap.

## 6. Bronze contracts

Bronze stores the imported source data before business-level normalization.

Required metadata:

- `batch_id`
- `source_file`
- `ingested_at_utc`
- `source_row_number` or equivalent stable source-row reference

Source records must remain traceable to their original CSV rows.

CSV parsing failures must not silently disappear. They are reported and retained in an appropriate rejection dataset or processing report.

Bronze may contain logically invalid records.

## 7. Silver contracts

Silver contains records that have passed mandatory validation and deterministic deduplication.

Silver transactions must have:

- A unique transaction ID.
- A valid customer reference.
- A supported status.
- A positive amount.
- A valid timestamp.
- A supported country code.
- A consistent currency.
- A valid device identifier format.

Unknown device ownership is not a data-quality error if the referenced customer exists.

The enriched dataset contains `known_device`, indicating whether a device/customer ownership pair exists in the registered-device dataset.

## 8. Gold contracts

Gold produces:

- `scored_transactions`
- `fraud_alerts`
- `customer_risk_summary`
- `rule_statistics`
- `daily_statistics`

Only transactions with at least one triggered rule enter `fraud_alerts`.

All valid analyzed transactions, including those with score zero, enter `scored_transactions`.

Gold datasets must remain internally consistent.

## 9. Batch and execution strategy

V1 processes complete batches.

A batch is identified by a stable `batch_id`, determined from a supplied parameter or deterministic input manifest.

The application must reject unsafe output-directory reuse and must not mix results from unrelated batches.

### Rerun policy

- Outputs are generated in a batch-specific location.
- A successful rerun replaces that batch's derived outputs atomically where practical.
- A failed rerun must not be presented as a successful completed batch.
- Published datasets must not append duplicates merely because the pipeline was re-executed.

## 10. Processing time

All transaction timestamps represent UTC instants.

- The source timestamp uses ISO 8601 with an explicit UTC indicator.
- Temporal rule calculations use actual elapsed time.
- The Spark session uses UTC.
- Daily summaries use UTC calendar dates.
- The dashboard labels times as UTC unless explicitly converted for display.

## 11. Local resource strategy

Default execution uses Spark local mode.

Start with one small deterministic dataset.

Scale gradually to:

1. Controlled fixture.
2. 1,000 transactions.
3. 50,000 transactions.
4. Optional 500,000+ benchmark.

Avoid unnecessary repartitioning and driver-side `collect()` operations.

## 12. Failure handling

Each stage must either:

- Complete and report its outputs, or
- Fail with an actionable error and a nonzero process exit status.

A fatal schema/configuration issue stops the pipeline.

Individual invalid source rows follow the documented rejection policy.

Unexpected exceptions must not be silently ignored.

## 13. Observability

Pipeline logs should record:

- Batch ID.
- Stage name.
- Start and end times.
- Input record counts.
- Valid record counts.
- Rejected and duplicate counts.
- Scored transaction count.
- Alert count.
- Rule activation counts.
- Processing duration.
- Failure details.

Counts must represent documented measurement points.

## 14. Security and privacy

- All data is synthetic.
- No secrets stored in source code.
- No private credentials required.
- `.gitignore` excludes generated large datasets and local environment files.
- A small non-sensitive sample may be committed for reproducibility.

## 15. Architecture boundaries

V1 does not require:

- Kafka.
- Airflow.
- Docker.
- PostgreSQL.
- REST APIs.
- Cloud services.
- Kubernetes.
- Stream processing.

These technologies may be evaluated after V1.0 without changing the core fraud-rule specification.
