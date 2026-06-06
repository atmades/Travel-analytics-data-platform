# ADR-XXX: Title

## Status

Proposed / Accepted / Deprecated

---

# Context

Describe the problem, constraints, and background.

Questions to answer:

- What problem are we solving?
- What constraints exist?
- What operational concerns exist?
- Why is this important?

---

# Decision

Describe the chosen solution.

Include:

- architecture approach;
- technologies;
- data flow;
- ownership boundaries.

---

# Alternatives Considered

Describe alternatives and why they were rejected.

Example:

- validate-before-raw;
- immutable raw layer;
- object storage first;
- direct API → warehouse.

---

# Consequences

Describe the impact.

Positive:

- replayability;
- auditability;
- operational simplicity.

Negative / trade-offs:

- increased storage;
- more complex pipelines;
- higher latency.

---

# Related Components

List related:

- DAGs
- tables
- services
- APIs
- adapters
- Kafka topics

---

# Related Documents

- architecture.md
- data_flow.md
- roadmap.md