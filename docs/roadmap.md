# Implementation Roadmap — Fraud Pattern Detector

## 1. Development strategy

Use incremental, test-driven development.

Do not start several major phases simultaneously.

Every phase should produce a working, testable and documented result.

Statuses:

- TODO: Not started.
- IN PROGRESS: Being implemented.
- BLOCKED: Cannot continue.
- DONE: Accepted and verified.

## 2. Global release milestones

| Milestone | Scope |
|---|---|
| M0 | Approved specifications |
| M1 | Working Scala/Spark environment |
| M2 | Synthetic dataset generation |
| M3 | Bronze and Silver pipeline |
| M4 | Five detection rules |
| M5 | Risk scoring and Gold |
| M6 | Automated verification |
| M7 | Dashboard and observability |
| M8 | Performance, documentation and V1 release |

## 3. Phase 0 — Specification

**Status:** DONE

### Tasks

- [x] Create documentation directory.
- [x] Review project scope.
- [x] Approve architecture.
- [x] Approve input schemas.
- [x] Approve detection rule semantics.
- [x] Approve quality policies.
- [x] Approve technological boundaries.
- [x] Approve testing strategy.
- [x] Approve implementation roadmap.
- [x] Audit specifications and apply approved corrections.

### Acceptance

All specifications are internally consistent and ready for implementation.

## 4. Phase 1 — Scala & Spark Development Environment (`spark-pipeline/`)

**Status:** DONE

### 1.1 Runtime compatibility

Select mutually supported versions of Java (17), Scala 3 (3.3.3), Spark (3.5.1), sbt (2.0.9) and ScalaTest.

### 1.2 Project initialization

Create `spark-pipeline/` sbt project structure and dependency definitions.

### 1.3 Application entry point

Build a minimal `spark-pipeline/src/main/scala/frauddetector/Main.scala`.

### 1.4 Spark initialization

Start and close a SparkSession in local mode under UTC session configuration.

### 1.5 Environment validation

Execute a simple DataFrame transformation and verify `sbt test`.

### Acceptance

- `sbt compile` succeeds in `spark-pipeline/`.
- `sbt test` succeeds.
- Spark starts and shuts down cleanly on Java 17.
- A basic DataFrame operation executes.

## 5. Phase 2 — Python Synthetic Generator (`generator/`)

**Status:** DONE

### 2.1 Customer generator

Generate customers and risk profiles in Python.

### 2.2 Registered-device generator

Generate customer-owned devices and registration dates.

### 2.3 Transaction generator

Generate ordinary transaction activity.

### 2.4 Scenario injection

Inject controlled behavioral scenarios for R001–R005.

### 2.5 Data-quality injection

Inject selected invalid values and duplicates (Q001–Q016, Q009A/Q009B).

### 2.6 Reproducibility

Add CLI seed support and deterministic scenario generation (`generate.py`).

### 2.7 Generator reporting

Export `generator_manifest.json` and multi-rule `scenario_expected.csv`.

### Acceptance

Development and full datasets generate deterministically from `python generate.py`.

## 6. Phase 3 — Controlled Data Fixture (`data/sample/`)

**Status:** DONE

### Tasks

- [x] Create customer sample.
- [x] Create registered-device sample.
- [x] Create transaction sample.
- [x] Include normal behavior.
- [x] Include isolated R001–R005 scenarios.
- [x] Include invalid data (Q004, Q005, Q006, Q009A, Q009B).
- [x] Document expected rule outcomes in scenario_expected.csv.
- [x] Validate fixture consistency with Spark test suite (SampleFixtureSpec).

### Acceptance

Controlled input rows and expected results can be manually inspected and validated via Spark.

### 3.3 Transaction generator

Generate ordinary transaction activity.

### 3.4 Scenario injection

Inject controlled behavioral scenarios.

### 3.5 Data-quality injection

Inject selected invalid values and duplicates.

### 3.6 Reproducibility

Add seed support and deterministic scenario generation.

### 3.7 Generator reporting

Export manifests, quality-defect counts and expected scenarios.

### Acceptance

Development and full datasets generate correctly from documented commands.

Repeated generation with the same seed produces equivalent business data.

## 7. Phase 4 — Bronze ingestion

**Status:** DONE

### 4.1 Spark schemas

Implement explicit source schemas.

### 4.2 Readers

Implement customer, device and transaction readers.

### 4.3 Parsing diagnostics

Capture malformed source records.

### 4.4 Metadata

Add batch and source metadata.

### 4.5 Parquet output

Persist Bronze datasets.

### 4.6 Tests

Verify loading, metadata and parsing errors.

### Acceptance

All three source datasets can be ingested and traced to Bronze.

## 8. Phase 5 — Data quality

**Status:** DONE

### 5.1 Structural checks

Validate required fields and types.

### 5.2 Semantic checks

Validate amounts, dates, catalogs and identifiers.

### 5.3 Reference checks

Validate customer and device registry relationships.

### 5.4 Deduplication

Implement exact and conflicting duplicate policies.

### 5.5 Quarantine

Persist rejected records with error codes.

### 5.6 Reconciliation

Produce valid, invalid and duplicate counts.

### 5.7 Tests

Test the full quality matrix.

### Acceptance

Accepted and excluded source rows reconcile, and invalid records do not reach Silver.

## 9. Phase 6 — Silver preparation

**Status:** DONE

### 6.1 Clean source datasets

Persist valid customers, devices and transactions.

### 6.2 Customer enrichment

Join transactions with customer profiles.

### 6.3 Device matching

Resolve registered ownership at event time.

### 6.4 Spark SQL

Create at least one meaningful Spark SQL transformation or validation query.

### 6.5 Cardinality tests

Verify one enriched row per unique valid transaction.

### Acceptance

Silver is clean, typed, unique and ready for detection.

## 10. Phase 7 — R001 Transaction Burst

**Status:** DONE

### Tasks

- [x] Implement approved-transaction filtering.
- [x] Implement 10-minute window counts.
- [x] Handle same-timestamp events.
- [x] Produce R001 Boolean flag.
- [x] Implement positive tests.
- [x] Implement negative tests.
- [x] Implement boundary tests.

### Acceptance

R001 matches the specification on controlled fixtures.

## 11. Phase 8 — R002 High Amount

**Status:** DONE

### Tasks

- [x] Load thresholds by customer profile.
- [x] Apply decimal comparisons.
- [x] Exclude declined transactions.
- [x] Produce R002 flag.
- [x] Test equality boundaries.
- [x] Test all customer profiles.

### Acceptance

R002 matches configured thresholds exactly.

## 12. Phase 9 — R003 Rapid Country Change

**Status:** DONE

### Tasks

- [x] Order approved customer transactions.
- [x] Implement deterministic tie-breaking.
- [x] Compare consecutive approved events.
- [x] Calculate elapsed time.
- [x] Compare countries.
- [x] Produce R003 flag.
- [x] Test 60-minute boundaries.

### Acceptance

R003 handles temporal sequencing correctly.

## 13. Phase 10 — R004 Repeated Declines

**Status:** DONE

### Tasks

- [x] Filter declined transactions.
- [x] Implement 15-minute window.
- [x] Count declines per customer.
- [x] Produce R004 flag.
- [x] Test positive, negative and boundary cases.

### Acceptance

R004 matches documented window semantics.

## 14. Phase 11 — R005 Unknown Device

**Status:** DONE

### Tasks

- [x] Verify device ownership status.
- [x] Respect registration timestamps.
- [x] Combine unknown device with R001–R004.
- [x] Produce R005 flag.
- [x] Test known, unknown and late-registered devices.

### Acceptance

R005 never activates solely because a device is unknown.

## 15. Phase 12 — Risk scoring

**Status:** DONE

### Tasks

- [x] Load scores from configuration.
- [x] Calculate transaction scores.
- [x] Apply severity bands.
- [x] Generate ordered rule ID arrays.
- [x] Generate explanations.
- [x] Validate score boundaries.
- [x] Test combined-rule scenarios.

### Acceptance

Every valid transaction has correct non-null flags, score and risk level.

## 16. Phase 13 — Gold analytics

**Status:** DONE

### Tasks

- [x] Publish scored transactions.
- [x] Publish fraud alerts.
- [ ] Publish customer risk summary.
- [ ] Publish rule statistics.
- [ ] Publish daily statistics.
- [x] Verify aggregate reconciliation.
- [x] Verify uniqueness.
- [x] Test rerun behavior.

### Acceptance

All Gold datasets are consistent and reproducible.

## 17. Phase 14 — Configuration and monitoring

**Status:** DONE

### Tasks

- [x] Centralize configuration.
- [x] Validate configuration.
- [x] Add stage-level logging.
- [x] Track row counts.
- [x] Track processing durations.
- [x] Report rule activations.
- [x] Implement consistent failure handling.
- [x] Create batch completion report.

### Acceptance

Pipeline operation can be understood from logs and output reports.

## 18. Phase 15 — Dashboard export

**Status:** DONE

### Tasks

- [x] Define dashboard export contracts.
- [x] Export KPIs from Gold.
- [x] Export rule counts.
- [x] Export daily statistics.
- [x] Export alert records.
- [x] Create export manifest.
- [x] Validate export reconciliation.

### Acceptance

Dashboard files accurately reflect Gold without requiring Spark at viewing time.

## 19. Phase 16 — Dashboard implementation

**Status:** DONE

### Tasks

- [x] Create responsive HTML layout.
- [x] Display principal KPIs.
- [x] Implement risk distribution chart.
- [x] Implement rule frequency chart.
- [x] Implement alerts-over-time chart.
- [x] Implement customer risk summary.
- [x] Implement filterable alert table.
- [x] Add rule explanations.
- [x] Document local dashboard viewing.

### Acceptance

The dashboard loads generated data and presents consistent, understandable results.

## 20. Phase 17 — Complete integration tests

**Status:** TODO

### Tasks

- [ ] Execute controlled end-to-end dataset.
- [ ] Compare expected flags.
- [ ] Validate quality counts.
- [ ] Validate Gold invariants.
- [ ] Validate dashboard exports.
- [ ] Test failed configurations.
- [ ] Test repeat execution.

### Acceptance

The complete pipeline passes all required automated checks.

## 21. Phase 18 — Performance evaluation

**Status:** TODO

### Tasks

- [ ] Benchmark 1,000 transactions.
- [ ] Benchmark 50,000 transactions.
- [ ] Inspect Spark execution plans.
- [ ] Review unnecessary shuffles.
- [ ] Record runtime and environment.
- [ ] Optionally benchmark 500,000 transactions.
- [ ] Document any optimization.

### Acceptance

A reproducible performance report is available.

## 22. Phase 19 — Portfolio presentation

**Status:** TODO

### Tasks

- [ ] Write professional root README.
- [ ] Add system architecture diagram.
- [ ] Document installation.
- [ ] Document generation and execution.
- [ ] Document rule examples.
- [ ] Explain limitations.
- [ ] Add dashboard screenshots.
- [ ] Include validated results.
- [ ] Add licensing information.
- [ ] Clean generated artifacts.
- [ ] Verify repository from a clean checkout.

### Acceptance

A technical reviewer can understand, execute and evaluate the repository.

## 23. Phase 20 — V1.0 release

**Status:** TODO

### Release checklist

- [ ] All mandatory features complete.
- [ ] All automated tests pass.
- [ ] No unresolved blocking defects.
- [ ] Documentation matches implementation.
- [ ] No real personal data.
- [ ] No credentials.
- [ ] No unintended generated large files.
- [ ] Fresh installation instructions verified.
- [ ] Screenshots correspond to actual results.
- [ ] Git release tag `v1.0.0` created.

## 24. Rules for phase progression

Never move to the next phase solely because an AI agent reports completion.

The developer must inspect:

- Changed files.
- Executed commands.
- Test results.
- Output data.
- Documentation changes.

A phase can be marked DONE only after acceptance evidence is available.

## 25. Optional future milestones

After V1.0:

- Evaluate Spark MLlib.
- Add incremental processing.
- Introduce an orchestration tool.
- Add streaming.
- Compare synthetic datasets with public datasets.
- Experiment with cloud deployment.
- Evaluate rule performance with independently labeled data.

These are separate future initiatives, not unfinished V1 requirements.
