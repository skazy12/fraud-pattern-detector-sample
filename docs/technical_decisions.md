# Technical Decisions — Fraud Pattern Detector

## TD-001 — Scala as the main processing language

**Decision:** Implement data ingestion, transformations, quality validation, rules and scoring in Scala.

**Reason:** Demonstrate Scala and JVM-based Spark development.

Python is restricted primarily to synthetic-data generation and supporting scripts.

## TD-002 — Apache Spark

**Decision:** Use Apache Spark for batch data processing.

**Reason:** Demonstrate distributed DataFrame operations, Spark SQL, joins, aggregations, window functions and columnar storage.

Avoid using Spark merely as a wrapper around driver-side Scala collections.

## TD-003 — Version compatibility

**Decision:** Maintain a pinned, explicitly tested toolchain based on system capabilities.

Pinned versions for Fraud Pattern Detector:
- **Java:** OpenJDK 17 (Temurin 17.0.19)
- **Scala:** Scala 3 (3.3.3 LTS)
- **sbt:** sbt 2.0.9 runner
- **Apache Spark:** 3.5.1 (with Java 17 `--add-opens` JVM options)
- **Python:** Python 3.14.7 (for `generator/`)
- **ScalaTest:** 3.2.18

The build file `spark-pipeline/build.sbt` and execution environment use these mutually compatible versions.

## TD-004 — Local-first execution

**Decision:** Use Spark local mode.

**Reason:**

- Zero mandatory cloud cost.
- Easier debugging.
- Simplified reproduction.
- Sufficient for the initial dataset volume.

A cloud deployment is optional after V1.

## TD-005 — sbt build

**Decision:** Use sbt to manage Scala compilation, dependencies and test execution.

Source code follows conventional sbt directory structure.

## TD-006 — Python + Faker generation

**Decision:** Use Python and Faker for synthetic-data generation.

**Reason:** Faster development of reproducible synthetic scenarios.

Generated data must have deterministic seeds and explicit validation.

## TD-007 — CSV and Parquet

**Decision:**

- CSV for raw generated input.
- Parquet for Bronze, Silver and Gold persisted datasets.
- JSON/CSV for lightweight dashboard exports.

**Reason:** Demonstrate schema handling and columnar analytical storage.

## TD-008 — Explicit schemas

**Decision:** Use explicit schemas for the three raw datasets.

No reliance on Spark schema inference in the final pipeline.

Malformed inputs must be detectable and traceable.

## TD-009 — DataFrame API and Spark SQL

**Decision:** Use both.

DataFrame API should handle reusable transformations, validation and rule feature construction.

Spark SQL should appear in meaningful analytical queries or aggregations.

Do not duplicate implementations solely to claim use of both interfaces.

## TD-010 — Modular rule engine

**Decision:** Implement logically independent rule modules with stable identifiers.

Rules receive prepared DataFrames or documented feature datasets and return deterministic rule results.

Do not implement the entire engine as one large method.

Avoid building an unnecessary generic plugin framework.

## TD-011 — Batch processing

**Decision:** Full-batch processing for V1.

No streaming, event bus or real-time API is required.

All temporal rules use transaction event time within the batch.

## TD-012 — Time handling

**Decision:**

- UTC everywhere inside the data pipeline.
- Explicitly configured UTC Spark session.
- ISO 8601 UTC source timestamps.
- Numeric timestamps for elapsed-time range calculations where appropriate.
- Documented tie-breaking rules.

Never rely on the machine's default local timezone.

## TD-013 — Financial precision

**Decision:** Use decimal monetary types.

Avoid binary floating-point for financial thresholds and amount calculations.

## TD-014 — Logging

**Decision:** Use JVM logging and structured, understandable pipeline messages.

At minimum, log batch ID, stage, counts, duration and failures.

Avoid logging all transaction rows.

## TD-015 — Configuration

**Decision:** Separate environment paths and detection thresholds from business logic.

Recommended files:

- `application.conf`
- `rules.conf`

Configuration is loaded and validated at startup.

Default configurations are version controlled.

Secrets, if introduced in future releases, must never be committed.

## TD-016 — ScalaTest

**Decision:** Use ScalaTest for the main Scala test suite.

Tests include:

- Pure configuration or scoring tests where practical.
- Spark transformation tests.
- Rule-specific temporal tests.
- Quality tests.
- Integration tests.
- Batch consistency tests.

## TD-017 — Static dashboard

**Decision:** HTML, CSS, JavaScript and Plotly.

**Reason:** Keep visualization separate from the Scala/Spark backend without creating another large application stack.

Dashboard exports must be generated from Gold data rather than hardcoded.

## TD-018 — No mandatory database

**Decision:** Do not use PostgreSQL, MongoDB or another database in V1.

Parquet is the persistence format.

A future database extension can be evaluated independently.

## TD-019 — No mandatory Docker

**Decision:** Begin with native local tooling.

Docker may be introduced only if local setup proves impractical or containerization becomes a deliberate later objective.

The main portfolio learning objective remains Scala and Spark.

## TD-020 — Git strategy

**Decision:**

- Use one Git repository.
- Work in small feature branches when practical.
- Make one or more coherent commits per completed phase.
- Avoid committing generated build artifacts.
- Keep deterministic sample fixtures.
- Document significant technical changes.

## TD-021 — Dependency management

- Pin dependency versions.
- Avoid unnecessary packages.
- Avoid dynamic dependency versions in the final build.
- Document installation requirements.
- Never allow an agent to add a dependency without explaining its purpose.

## TD-022 — Performance

Initial optimization priorities:

1. Correctness.
2. Deterministic results.
3. Reasonable local execution time.
4. Understandable Spark query plans.
5. Reducing avoidable shuffles and materialization.

Do not add caching or repartitioning without a measurable reason.

Performance comparisons should use identical input data, configuration and environment.

## TD-023 — Idempotency

Derived outputs must be batch-specific and reproducible.

A rerun must not append duplicate alerts.

Use a documented publication strategy to prevent partial outputs being mistaken for completed outputs.

## TD-024 — Error handling

Configuration and infrastructure errors stop the pipeline.

Record-level quality errors are quarantined where supported.

Do not suppress exceptions or report success after a failed stage.

## TD-025 — Scope control

No additional services, frameworks or architectural layers may be introduced merely because an AI coding agent recommends them.

Every scope expansion requires a documented decision.

## TD-026 — Documentation language

Technical documentation and code identifiers use English.

Developer explanations and learning notes may use Spanish.

This supports an internationally understandable GitHub repository.

## TD-027 — Planned improvement process

When a technical decision changes:

1. Describe the previous decision.
2. Explain the problem.
3. Propose the replacement.
4. Assess affected files and tests.
5. Request approval.
6. Update specifications.
7. Implement and verify the change.

## TD-028 — Apache Spark and sbt Caching Strategy

**Decision:** Enforce a strict anti-garbage caching policy across sbt build and Spark processing layers.

1. **sbt build caching:** Use `sbt clean` to remove local build target files in `spark-pipeline/target/` when performing clean builds. Use `sbt shutdown` to close background sbt server instances.
2. **Spark DataFrame caching:** Never call `df.cache()` or `df.persist()` unless the DataFrame is evaluated in 2 or more downstream Spark actions.
3. **Mandatory unpersist:** Always release cached DataFrames via `df.unpersist()` in a `try-finally` block once downstream actions complete.
4. **Session teardown:** `spark.stop()` automatically clears the Spark `BlockManager` and RAM/Disk temporary allocations.
