# Day 10 — Partitioning

## Objective

The goal of Day 10 is to understand:

- Partition counts
- `repartition`
- `coalesce`
- `partitionBy`
- Increasing and decreasing partitions
- Optimizing a dataset with too few partitions

---

## 1. What is a Partition?

A partition is a portion of an RDD.

Spark divides data into partitions so that different portions can be processed in parallel.

In this project, the customer dataset initially contains only one partition.

```text
Initial partitions = 1
