# 🌊 Streaming Ingestion Evaluation Kit

A reusable, customer-agnostic kit for evaluating a real-time streaming ingestion + serving stack:

```
message bus  →  open table format on object storage  →  analytics serving
```

Use it when comparing candidate architectures (open-lakehouse stacks, real-time OLAP engines, or hybrids) against a concrete set of workload requirements.

## What's in the kit

| Artifact | Purpose |
|---|---|
| [requirements_one_pager.md](requirements_one_pager.md) | Capture the workload requirements once, in numbers, before anyone argues about engines |
| [evaluation_scorecard.md](evaluation_scorecard.md) | Score every candidate stack against the same measured criteria |
| [decision_grid.md](decision_grid.md) | Break the architecture into four independent modules so each is decided on its own criteria |

## How to use it

1. **Copy** the three templates into your (private!) working space.
2. **Fill in the `{{placeholders}}`** in the requirements one-pager with real numbers from the workload owner. Everything else keys off these values.
3. **List your candidate options** in the decision grid, one module at a time.
4. **Run the measurements** described in the scorecard against each end-to-end candidate, and record results.
5. **Decide per module** — open storage decouples ingestion compute from query compute, so you don't have to buy one vendor's answer to all four questions.

## Conventions

- Placeholders use `{{snake_case}}` tokens (e.g., `{{events_per_sec_steady}}`, `{{freshness_slo}}`). Anything not yet known is marked `TBD`.
- This kit is **generic by design**: no customer names, no customer-specific workload numbers. Keep filled-in copies out of public repos.
- **GA-only framing**: the recommended path uses only generally-available features. Preview/beta engines and features are recorded as fast-follows — never on the critical path.
