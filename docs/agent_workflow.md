# AI Agent Development Workflow — Fraud Pattern Detector

## 1. Purpose

Define a controlled AI-assisted development workflow for Fraud Pattern Detector.

AI coding agents assist with planning, implementation, debugging, testing and code review.

The human developer owns architectural decisions, business semantics and final acceptance.

## 2. General agent rules

1. Read relevant project documentation before implementing.
2. Respect approved schemas and rule semantics.
3. Work only on the requested phase.
4. Prefer small, understandable changes.
5. Do not add unrelated features.
6. Do not silently refactor unrelated components.
7. Do not change dependency versions without approval.
8. Do not modify business rules without approval.
9. Explain technical decisions.
10. Report uncertainties and blockers.
11. Run relevant tests when execution is available.
12. Never claim tests passed without executing them.
13. Do not proceed automatically to the next phase.

## 3. Documentation-first workflow

Before implementation, identify:

- Current roadmap phase.
- Relevant documentation files.
- Required source files.
- Expected output.
- Acceptance criteria.
- Test requirements.
- Existing functionality that must remain unchanged.

If documentation is incomplete, request clarification.

## 4. Standard development cycle

### Step A — Analysis

The agent reviews requirements and the relevant code.

No changes.

Expected response:

- Understanding of the objective.
- Proposed approach.
- Files to create or modify.
- Technical risks.
- Required tests.
- Open decisions.

### Step B — Human approval

The developer confirms or changes the proposed scope.

### Step C — Implementation

The agent implements only the approved task.

Expected:

- Minimal code changes.
- Clear file organization.
- Appropriate naming.
- No unnecessary abstractions.
- No unrelated features.

### Step D — Verification

Run:

- Compilation.
- Relevant unit tests.
- Relevant Spark tests.
- Manual checks if needed.

If tests cannot execute, explicitly say why.

### Step E — Technical explanation

Explain:

- What changed.
- Why it changed.
- Important Scala syntax.
- Spark transformations and actions.
- Any joins or window functions.
- Expected input and output.
- Performance implications.

### Step F — Code review

Review for:

- Correctness.
- Readability.
- Duplicate code.
- Configuration issues.
- DataFrame mistakes.
- Unnecessary Spark shuffles.
- Nondeterministic behavior.
- Missing tests.
- Schema inconsistencies.

### Step G — Acceptance

The developer inspects the result.

If approved:

- Update documentation.
- Update roadmap status.
- Create a coherent Git commit.

Do not create a release tag unless specifically requested.

## 5. Agent roles

These are working roles, not necessarily separate AI subscriptions or processes.

### Architect

Responsibilities:

- Analyze requirements.
- Propose implementation.
- Identify dependencies.
- Highlight inconsistencies.
- Evaluate design trade-offs.

Does not write code unless specifically requested.

### Implementer

Responsibilities:

- Create code.
- Follow specifications.
- Keep changes limited.
- Add required tests.
- Provide execution instructions.

### Reviewer

Responsibilities:

- Inspect implementation.
- Check business correctness.
- Identify technical debt.
- Review Spark processing semantics.
- Recommend targeted improvements.

Does not modify files unless requested.

### Tutor

Responsibilities:

- Explain Scala and Spark concepts.
- Use examples from the implemented code.
- Suggest small learning exercises.
- Help the developer understand code without relying blindly on AI.

## 6. Prompt template — Planning

```text
We are working on Fraud Pattern Detector.

Current phase: [PHASE]

Read the relevant documents under /docs and inspect
the existing implementation.

Do not modify files yet.

Provide:
1. Requirements summary.
2. Proposed implementation.
3. Files affected.
4. Dependencies.
5. Potential problems.
6. Test strategy.
7. Acceptance criteria.

Do not move to the next phase.
```

## 7. Prompt template — Implementation

```text
Implement only the approved scope for phase [PHASE].

Constraints:
- Follow /docs specifications.
- Preserve working functionality.
- Do not add unrelated features.
- Do not change schemas or fraud rules.
- Keep code readable and modular.
- Add relevant automated tests.

After implementation:
1. Summarize changed files.
2. Show commands executed.
3. Report test results.
4. Explain any unresolved issue.
5. Stop.

Do not continue to another phase.
```

## 8. Prompt template — Code review

```text
Review the implementation of phase [PHASE].

Focus on:
- Correctness.
- Specification compliance.
- Scala conventions.
- Spark DataFrame semantics.
- Window correctness.
- Determinism.
- Data quality.
- Test coverage.
- Performance risks.

Do not modify code.

Classify findings by:
CRITICAL
HIGH
MEDIUM
LOW

For each finding, explain the cause and
recommend a focused correction.
```

## 9. Prompt template — Learning

```text
Act as a Scala and Apache Spark tutor.

Explain the implementation completed in phase [PHASE].

Cover:
1. Purpose of the code.
2. Relevant Scala syntax.
3. Spark operations.
4. Data transformations.
5. Input/output example.
6. Alternative implementations.
7. Common mistakes.
8. Two exercises for me to solve independently.

Do not modify files.
```

## 10. Prompt template — Debugging

```text
A problem occurred in phase [PHASE].

Error:
[ERROR DETAILS]

Expected behavior:
[EXPECTED]

Actual behavior:
[ACTUAL]

Inspect relevant code and specifications.

Find the root cause.
Propose the smallest safe correction.
Do not refactor unrelated code.
Add a regression test when appropriate.
```

## 11. Context management

Do not load the entire repository into every agent conversation unnecessarily.

Provide the agent with:

- Current phase.
- Relevant documentation.
- Relevant source files.
- Current error or objective.
- Explicit constraints.

Avoid lengthy chat histories when current files already contain the necessary context.

## 12. Working with limited AI usage

Prefer tasks that are:

- Narrowly scoped.
- Independently testable.
- Clearly specified.
- Small enough to review.
- Based on existing documentation.

Use lightweight analysis for straightforward tasks.

Reserve more intensive reasoning for:

- Architecture.
- Ambiguous business logic.
- Difficult Spark window behavior.
- Performance analysis.
- Complex debugging.
- Major cross-module changes.

No particular model is mandatory for this workflow.

## 13. Git workflow

Recommended branch names:

- `feat/synthetic-generator`
- `feat/bronze-ingestion`
- `feat/data-quality`
- `feat/rule-r001`
- `feat/risk-scoring`
- `feat/dashboard`

Suggested commit prefixes:

- `feat:`
- `fix:`
- `test:`
- `docs:`
- `refactor:`
- `chore:`

Never automatically commit secrets or generated large datasets.

## 14. Agent permissions

Allowed with approved scope:

- Read project files.
- Modify relevant source files.
- Create relevant tests.
- Run local build/test commands.
- Update relevant technical documentation.

Require human approval:

- Delete important files.
- Change rule semantics.
- Change schemas.
- Add major dependencies.
- Restructure the repository.
- Introduce cloud services.
- Change the roadmap scope.
- Publish releases.
- Perform destructive operations.

## 15. Phase completion report

Every implementation task should end with:

```text
PHASE:
STATUS:

FILES CREATED:
FILES MODIFIED:

IMPLEMENTED:

TESTS EXECUTED:
TEST RESULTS:

MANUAL VALIDATION:

DOCUMENTATION UPDATED:

KNOWN ISSUES:

NEXT RECOMMENDED TASK:
```

A test report must distinguish tests executed from tests merely suggested.

## 16. Final principle

AI agents accelerate implementation.

The developer remains responsible for understanding, validating, explaining and maintaining the system.

A completed feature is one the developer can run, test and explain without relying exclusively on the agent.
