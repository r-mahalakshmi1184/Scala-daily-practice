
---

# Day 27 — Real-Time Banking Project

```markdown
# Day 27 – Real-Time Banking Project

## Overview

This practice focuses on designing a real-time banking transaction processing system using Spark concepts.

## Project Scenario

Process continuously arriving banking transaction events and generate account-level analytics while identifying suspicious transaction bursts.

## Pipeline

```text
Bank Transaction Events
          ↓
      Streaming
          ↓
   Transaction Parsing
          ↓
 Account-Level Aggregation
          ↓
 Suspicious Burst Detection
          ↓
 Branch/Risk Enrichment
          ↓
      Analytics Output
