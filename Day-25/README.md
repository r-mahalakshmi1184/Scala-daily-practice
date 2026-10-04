
---

# Day 25 — Window Operations

```markdown
# Day 25 – Window Operations

## Overview

This practice focuses on window-based stream processing for calculating rolling metrics over a specific time period.

## Objectives

- Understand batch interval
- Understand window size
- Understand sliding interval
- Use `countByWindow`
- Use `reduceByKeyAndWindow`
- Calculate rolling sales totals

## Scenario

Detect a sudden increase in transactions during a 10-minute window.

## Window Concepts

```text
Batch Interval
      ↓
Window Size
      ↓
Sliding Interval
      ↓
Rolling Result
