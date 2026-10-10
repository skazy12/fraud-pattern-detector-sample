# Fraud Rules Specification — Fraud Pattern Detector

## 1. Purpose

Define deterministic and explainable suspicious-pattern detection rules.

Each rule evaluates transaction data and returns a Boolean flag per transaction.

Rules identify suspicious indicators, not confirmed fraudulent activity.

## 2. General principles

- Evaluate only valid unique Silver transactions.
- Use transaction event time in UTC.
- Use Spark DataFrame API and Spark SQL where suitable.
- Calculate rules through distributed operations.
- Preserve exactly one result per transaction.
- Store rule thresholds in configuration.
- Avoid nondeterministic ordering.
- Do not use future events to alter earlier transaction-level rule results.
- Treat all time windows as backward-looking and inclusive of the current event.
- Scores are heuristic points, not probability estimates.

## 3. Rule catalog

| ID | Name | Default score |
|---|---|---:|
| R001 | Transaction Burst | 20 |
| R002 | High Amount | 30 |
| R003 | Rapid Country Change | 40 |
| R004 | Repeated Declines | 15 |
| R005 | Unknown Device with Additional Indicator | 10 |

Maximum theoretical score: 115.

Theoretical maximum does not imply all rule combinations are equally likely.

## 4. R001 — Transaction Burst

### Objective

Identify unusually frequent approved transactions from the same customer.

### Eligible events

APPROVED transactions only.

### Window

10 elapsed minutes, including the current transaction.

For a transaction at time T, count approved transactions for the same customer in:

`[T - 10 minutes, T]`

### Trigger condition

`approved_transaction_count >= 5`

Equivalent to more than four approved transactions during the window.

### Score

20 points.

### Expected implementation

Spark window function partitioned by customer ID and ordered by a numeric UTC timestamp.

A bounded `rangeBetween` can be used with correct timestamp units.

### Deterministic treatment

Transactions with the exact same timestamp are considered simultaneous.

To avoid ambiguity about peer rows, all approved transactions at the same timestamp for one customer receive the same 10-minute count.

No transaction may be counted twice.

### Test cases

**Positive:** Five approved transactions in eight minutes.

**Negative:** Four approved transactions in ten minutes.

**Boundary:** Five approved transactions spanning exactly ten minutes trigger.

**Outside boundary:** Five transactions spanning ten minutes and one second do not necessarily trigger; count the actual events in each window.

**Status:** Declined transactions do not contribute.

### Output

`rule_r001: boolean`

## 5. R002 — High Amount

### Objective

Identify a large approved transaction relative to the customer's configured risk profile.

### Eligible events

APPROVED transactions only.

### Default threshold table

| Customer profile | Amount threshold |
|---|---:|
| LOW | 2,000 EUR |
| MEDIUM | 1,500 EUR |
| HIGH | 1,000 EUR |

### Trigger condition

`amount > configured_threshold`

Threshold equality does not trigger.

### Score

30 points.

### Test cases

- LOW customer, 2,001 EUR: true.
- LOW customer, 2,000 EUR: false.
- MEDIUM customer, 1,501 EUR: true.
- HIGH customer, 1,001 EUR: true.
- DECLINED transaction above threshold: false.

### Note

These are demonstration thresholds. They are not real banking fraud parameters.

The `risk_profile` is a synthetic business classification, not a real customer credit or fraud-risk assessment.

### Output

`rule_r002: boolean`

## 6. R003 — Rapid Country Change

### Objective

Identify two consecutive approved transactions by a customer occurring in different countries within a short elapsed-time interval.

### Eligible events

APPROVED transactions only.

### Window

60 elapsed minutes.

### Trigger condition

For the current approved transaction:

1. The single immediately preceding approved transaction (`lag(1)`) for the same customer has a different country.
2. Elapsed time between the current transaction and the immediately preceding approved transaction is greater than or equal to 0 minutes and less than or equal to 60 minutes.

### Simultaneous event convention

For equal timestamps, order by `transaction_id` ascending.

The immediately preceding event is determined using Spark `lag(country, 1)` over the window:
`PARTITION BY customer_id ORDER BY transaction_timestamp ASC, transaction_id ASC`

This is a deterministic sequencing convention comparing exclusively the immediately previous approved event.

### Expected implementation

Spark `lag(country, 1)` and `lag(transaction_timestamp, 1)` over an ordered customer window filtered to APPROVED transactions.

### Score

40 points.

### Positive example

10:00 UTC — ES  
10:35 UTC — FR

The second event triggers.

### Negative example

10:00 UTC — ES  
12:30 UTC — FR

The second event does not trigger.

### Boundary cases

- Exactly 60 minutes: true.
- 60 minutes and one second: false.
- Same country: false.
- No previous approved transaction: false.
- Declined transactions: ignored when identifying the previous approved transaction.

### Important limitation

The rule does not calculate geographic distance, travel speed or physical impossibility.

A rapid country change is a heuristic indicator, not a determination of impossible travel.

### Output

`rule_r003: boolean`

## 7. R004 — Repeated Declines

### Objective

Identify repeated declined transactions by the same customer.

### Eligible events

DECLINED transactions only.

### Window

15 elapsed minutes.

### Trigger condition

`declined_transaction_count >= 3`

The count includes the current event.

### Score

15 points.

### Same-timestamp convention

All declined transactions sharing a timestamp for one customer receive the same count for that event-time window.

### Positive example

12:00 — DECLINED  
12:05 — DECLINED  
12:09 — DECLINED

The third event triggers.

### Negative example

Two declines during the same 15-minute window.

### Boundary example

Three declined transactions spanning exactly 15 minutes trigger.

### Output

`rule_r004: boolean`

## 8. R005 — Unknown Device with Additional Indicator

### Objective

Identify transactions using a device that is not registered to the customer at the event time, when another suspicious indicator is also active.

### Device classification

`known_device = true` only if:

- `device_id` belongs to the same customer, and
- `registered_at <= transaction_timestamp`.

Otherwise `known_device = false`.

### Trigger condition

```text
NOT known_device
AND
(
  rule_r001
  OR rule_r002
  OR rule_r003
  OR rule_r004
)
```

### Score

10 additional points.

### Positive example

Unknown device and R002 triggered.

### Negative example

Unknown device with no other activated rules.

### Additional behavior

An unregistered device alone generates no R005 alert and contributes zero points.

The `known_device` column remains available for downstream analytics.

### Output

`rule_r005: boolean`

## 9. Rule evaluation sequence

1. Calculate R001.
2. Calculate R002.
3. Calculate R003.
4. Calculate R004.
5. Calculate R005 from the registered-device result and R001–R004 flags.
6. Calculate scoring.

R001–R004 may share precomputed features but must retain individually testable semantics.

R005 must not recursively depend on itself.

## 10. Scoring

Default calculation:

```text
risk_score =
  20 * R001
+ 30 * R002
+ 40 * R003
+ 15 * R004
+ 10 * R005
```

Where true evaluates to 1 and false evaluates to 0.

Scores use non-null Boolean flags.

No score is added for a known device or an inactive rule.

## 11. Severity classification

| Score | Risk level |
|---|---|
| 0–19 | LOW |
| 20–39 | MEDIUM |
| 40–59 | HIGH |
| 60 and above | CRITICAL |

These bands apply to transaction-level heuristic scores.

### Examples

| Triggered rules | Score | Level |
|---|---:|---|
| None | 0 | LOW |
| R004 | 15 | LOW |
| R001 | 20 | MEDIUM |
| R002 | 30 | MEDIUM |
| R003 | 40 | HIGH |
| R002 + R005 | 40 | HIGH |
| R001 + R003 | 60 | CRITICAL |
| R001 + R002 + R005 | 60 | CRITICAL |

**Important:** Severity LOW does not necessarily mean there was no rule activation. A transaction with R004 receives a LOW score of 15 and still belongs in the alert table.

## 12. Alert definition

An alert is created when:

`rule_r001 OR rule_r002 OR rule_r003 OR rule_r004 OR rule_r005`

Every analyzed transaction receives a score, including zero.

Only rule-activated transactions appear in `fraud_alerts`.

## 13. Rule explanations

Each alert must contain ordered rule IDs.

Recommended explanations:

- R001: High transaction frequency.
- R002: Amount exceeds customer profile threshold.
- R003: Rapid change of transaction country.
- R004: Repeated declined transactions.
- R005: Unknown device combined with another indicator.

Explanation labels are presentation metadata; they do not change detection behavior.

## 14. Configuration validation

At pipeline startup:

- Rule IDs must be unique.
- Scores must be nonnegative integers.
- Time windows must be positive.
- Count thresholds must be positive integers.
- Amount thresholds must be positive decimals.
- Every required risk profile must have a defined amount threshold.
- Score-band boundaries must be valid and nonoverlapping.

Invalid configuration is a fatal startup error.

## 15. False positives and evaluation limitations

Legitimate behavior may activate detection rules.

Examples include:

- Multiple valid purchases in a short period.
- Large authorized purchases.
- VPN-related country differences.
- Repeated password or payment errors.
- New customer devices.

The project must not claim real-world accuracy, precision, recall or false-positive rate based solely on injected synthetic scenarios.

Controlled fixture results may demonstrate implementation correctness, but not real-world fraud-detection effectiveness.
