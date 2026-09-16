# Day 9 — Pair RDD

## Objective

The goal of Day 9 is to understand:

- Key-value RDDs
- `reduceByKey`
- `groupByKey`
- `mapValues`
- Revenue aggregation by product
- Revenue aggregation by department
- Performance differences between `reduceByKey` and `groupByKey`
- Bank transaction aggregation by account ID

---

## 1. Pair RDD

A Pair RDD is an RDD containing key-value pairs.

Example:

```scala
(product, revenue)
