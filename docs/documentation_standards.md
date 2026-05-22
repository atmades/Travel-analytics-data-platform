# Documentation Standards

## Principles

The project follows production-style documentation practices.

Documentation should:

- explain architecture and decisions;
- stay versioned with the codebase;
- evolve together with the platform;
- document WHY, not only HOW;
- support onboarding and operations.

---

# Documentation Principles

## Audience-first

Each document should clearly target a specific audience:

- engineers;
- operators;
- reviewers;
- future contributors.

---

## Docs as Code

Documentation is stored in Git and versioned together with the platform.

All major architectural changes should include documentation updates.

---

## Living Documentation

Documentation must evolve together with the implementation.

Outdated documentation should be updated or removed.

---

## Explain WHY

Architecture documentation should explain:

- trade-offs;
- constraints;
- reasons for decisions;
- operational implications.

---

## Single Source of Truth

Avoid duplicated documentation.

Architecture decisions should be documented once using ADRs.

---

# Required Documentation

Major platform components should include:

- architecture documentation;
- data flow documentation;
- operational runbooks;
- architectural decisions (ADR);
- local setup instructions.

---

# ADR Rules

Every significant architectural decision should have:

- context;
- decision;
- alternatives considered;
- consequences.

ADR naming:

```text
001-immutable-raw-layer.md
002-adapter-layer.md
003-kafka-event-streaming.md
```

---

# Documentation Review Checklist

Before merging major features:

- architecture docs updated;
- roadmap updated;
- ADR added if needed;
- runbook updated;
- diagrams updated if architecture changed.