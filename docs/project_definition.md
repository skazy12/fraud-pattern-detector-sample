# Project Definition — Fraud Pattern Detector

## 1. Product identity

**Name:** Fraud Pattern Detector  
**Version:** 1.0  
**Category:** Data Engineering / Financial Transaction Monitoring  
**Execution:** Local batch processing  
**Primary language:** Scala  
**Processing engine:** Apache Spark

## 2. Problem statement

Financial institutions process large volumes of transactions. Manual inspection of every operation is inefficient and makes it difficult to systematically identify unusual patterns.

Common behavioral indicators include:

- Many transactions within a short period.
- Transactions exceeding configured amount thresholds.
- Rapid transaction activity across different countries.
- Repeated declined transactions.
- Activity involving unknown devices combined with other risk indicators.

These indicators do not independently prove fraudulent behavior.

## 3. Proposed solution

Develop a batch-oriented pipeline that:

1. Generates realistic synthetic banking transactions.
2. Ingests structured CSV datasets.
3. Validates data quality.
4. Cleans and enriches the information.
5. Applies five deterministic detection rules.
6. Assigns a risk score to each eligible transaction.
7. Generates analytical datasets and alerts.
8. Exports summarized information for an interactive dashboard.

## 4. Business objectives

**BO-01:** Identify potentially suspicious transaction patterns through reproducible rules.

**BO-02:** Generate interpretable results explaining which indicators contributed to a transaction's score.

**BO-03:** Summarize risk patterns by transaction, customer, rule and date.

**BO-04:** Demonstrate data quality handling and traceable processing.

**BO-05:** Provide a complete, reproducible portfolio project.

## 5. Technical objectives

**TO-01:** Use Scala for the main processing pipeline.

**TO-02:** Use Apache Spark DataFrame API and Spark SQL.

**TO-03:** Implement Bronze, Silver and Gold processing layers.

**TO-04:** Use explicitly defined input schemas.

**TO-05:** Persist intermediate and analytical datasets in Parquet.

**TO-06:** Use distributed DataFrame operations rather than driver-side row-by-row processing.

**TO-07:** Implement automated tests using ScalaTest.

**TO-08:** Provide consistent logging, configuration and error handling.

**TO-09:** Support full local execution without paid infrastructure.

**TO-10:** Document architecture, assumptions and limitations.

## 6. Target users

### Data engineer

Runs the pipeline, monitors processing results and inspects validation statistics.

### Fraud analyst (simulated role)

Reviews generated indicators, transaction risk scores and customer-level summaries.

### Technical reviewer

Examines code, architecture, reproducibility, testing and performance.

There is no authentication or role management in V1.

## 7. Dataset requirements

Target generated dataset:

| Dataset | Records |
|---|---:|
| Customers | 1,000 |
| Registered devices | 1,500 |
| Transactions | 50,000 |

A small controlled fixture must be available independently of the full dataset.

The dataset must include ordinary behavior, deliberately injected suspicious scenarios and known data-quality defects.

All customer, merchant, account and device information is fictitious.

## 8. Functional requirements

### FR-01 — Synthetic data generation

Generate reproducible, relationally coherent synthetic datasets using a fixed random seed.

### FR-02 — Ingestion

Read transactions, customers and devices using Apache Spark and explicit schemas.

### FR-03 — Bronze storage

Persist ingested records with traceability metadata.

### FR-04 — Data validation

Validate mandatory fields, supported values, temporal consistency, references and duplicate identifiers.

### FR-05 — Silver preparation

Create clean and consistently typed analytical datasets.

### FR-06 — Enrichment

Join transactions with customer attributes and device ownership information.

### FR-07 — Rules engine

Execute five independently identifiable detection rules: R001–R005.

### FR-08 — Risk scoring

Calculate deterministic transaction-level scores and risk levels.

### FR-09 — Gold datasets

Publish transaction alerts and aggregated analytical datasets.

### FR-10 — Dashboard export

Prepare lightweight data files for the dashboard.

### FR-11 — Reporting

Produce processing counts, rule counts and rejection statistics.

### FR-12 — Configuration

Allow rule thresholds and scores to be changed through configuration without modifying source code.

## 9. Non-functional requirements

**NFR-01 — Reproducibility:** Same seed, configuration, inputs and software environment must produce semantically equivalent results.

**NFR-02 — Idempotency:** Reprocessing the same batch must not create duplicated analytical records.

**NFR-03 — Maintainability:** Separate ingestion, validation, transformation, detection, scoring and output logic.

**NFR-04 — Testability:** Rules and validations must support isolated tests.

**NFR-05 — Explainability:** Every risk score must be traceable to rule identifiers.

**NFR-06 — Portability:** The project should be executable locally using documented prerequisites.

**NFR-07 — Privacy:** No real banking customer information.

**NFR-08 — Cost control:** No mandatory cloud services or paid APIs.

**NFR-09 — Observability:** Logs and processing metrics must identify failed stages.

**NFR-10 — Correctness:** Optimization must never change defined rule semantics.

## 10. Explicit scope exclusions

V1.0 will NOT include:

- Real-time fraud blocking.
- Production banking system integrations.
- Payment gateway integrations.
- Real customer data.
- Predictive machine learning.
- Kafka or streaming ingestion.
- Authentication and user management.
- Distributed cloud deployment.
- A mandatory SQL database.
- Production-level compliance certification.
- A real-world guarantee of fraud detection accuracy.

## 11. Expected deliverables

1. Source code repository.
2. Synthetic data generator.
3. Controlled validation fixture.
4. Configurable Scala/Spark batch pipeline.
5. Bronze, Silver and Gold datasets.
6. Five implemented detection rules.
7. Risk scoring logic.
8. Automated test suite.
9. Static dashboard.
10. Execution instructions.
11. Architecture documentation.
12. Demonstration screenshots.
13. Performance report.
14. Final portfolio README.

## 12. Success criteria

V1.0 is accepted when:

- All required components run in a clean local setup.
- Synthetic datasets are generated deterministically.
- Data-quality rejection and deduplication are verifiable.
- The five rules produce expected results on controlled fixtures.
- Scores and severities match the documented specification.
- Gold contains correct and consistently derived results.
- Dashboard values reconcile with Gold.
- Re-running a completed batch does not accumulate duplicates.
- Automated tests pass.
- The project can be demonstrated from its README.

## 13. Limitations

A rule trigger is a suspicious-pattern indicator, not a confirmed fraud event.

The synthetic scenario labels are not equivalent to independent real-world ground truth.

Risk scores are heuristic and are not calibrated probabilities of fraud.

Measured performance on local synthetic data must not be presented as production banking performance.

## 14. Portfolio positioning

The project should primarily demonstrate:

- Scala programming.
- Apache Spark.
- Spark SQL.
- DataFrame operations.
- Window functions.
- ETL processing.
- Data quality.
- Parquet.
- Rule-based analytics.
- Automated testing.
- Explainable risk scoring.

The project must be described truthfully as a portfolio prototype using synthetic financial transactions.
