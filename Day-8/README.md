# Day 8 — DAG and Spark Execution

## Objective

The goal of Day 8 is to understand:

- Spark jobs
- Spark stages
- Spark tasks
- RDD partitions
- DAG (Directed Acyclic Graph)
- Shuffle boundaries
- Narrow transformations
- Wide transformations
- Stage prediction for `reduceByKey`

---

## 1. Spark Execution Flow

Spark execution can be understood as:

```text
Action
   ↓
Job
   ↓
Stages
   ↓
Tasks
   ↓
Partitions
