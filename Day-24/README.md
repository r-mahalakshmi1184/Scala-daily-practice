
---

# Day 24 — Stateless vs Stateful Streaming

```markdown
# Day 24 – Stateless vs Stateful Streaming

## Overview

This practice focuses on the difference between stateless and stateful stream processing.

## Objectives

- Implement stateless transformations
- Understand stateful processing
- Track running counts by key
- Compare current-batch results with accumulated state

## Scenario

Maintain running transaction counts for individual bank accounts.

Example:

```text
Account C001 → Transaction 1
Account C001 → Transaction 2
Account C002 → Transaction 1
