
---

# Day 23 — DStreams Basics

```markdown
# Day 23 – DStreams Basics

## Overview

This practice introduces Spark Streaming using DStreams and demonstrates micro-batch stream processing.

## Objectives

- Create a StreamingContext
- Define a batch interval
- Read data from a socket or text stream
- Apply `map`
- Apply `filter`
- Apply `flatMap`
- Understand micro-batch processing

## Scenario

Process application log streams and count `ERROR` messages during every streaming interval.

## Streaming Flow

```text
Application Logs
       ↓
Streaming Input
       ↓
DStream
       ↓
map / filter / flatMap
       ↓
ERROR Count
       ↓
Streaming Output
