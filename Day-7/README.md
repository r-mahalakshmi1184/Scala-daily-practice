# Day 7 — Immutability, Lineage and Fault Tolerance

## Objective

The goal of Day 7 is to understand:

- RDD immutability
- RDD lineage
- Spark fault tolerance
- How Spark can recompute lost partitions

---

## 1. RDD Immutability

RDDs are immutable, which means an existing RDD cannot be changed after it is created.

In this program, every transformation creates a new RDD:

```text
numbers
   ↓ map
integers
   ↓ filter
evenNumbers
   ↓ map
multiplied
   ↓ filter
finalRDD
