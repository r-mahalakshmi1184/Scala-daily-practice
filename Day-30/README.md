# Day 30 - Final E-Commerce Analytics Capstone

## Objective

Build a final end-to-end Apache Spark project that combines the major concepts learned throughout the 30-day Scala and Spark practice.

This capstone demonstrates:

- Pair RDD
- Broadcast variable
- Accumulator
- Cache / Persist
- Partition tuning
- DataFrames
- Spark SQL
- Aggregations
- Window functions
- UDF
- Execution plan analysis
- Spark transformations and actions
- Shuffle and stage concepts

---

## Architecture

```text
                    E-Commerce Orders
                           |
                           v
                    Raw Data Ingestion
                           |
                           v
                    Data Cleaning
                           |
                           v
                 Total Amount Calculation
                           |
             +-------------+-------------+
             |                           |
             v                           v
        Pair RDD                    DataFrame
     Customer Revenue              Processing
             |                           |
             v                           v
       groupByKey()              Broadcast Product
       mapValues()                 Reference Data
                                         |
                                         v
                                  Enriched Orders
                                         |
                                         v
                                  Repartitioning
                                         |
                                         v
                                      UDF
                                         |
                                         v
                                  Aggregations
                                         |
                                         v
                                  Window Ranking
                                         |
                                         v
                                    Spark SQL
                                         |
                                         v
                                Final Analytics
