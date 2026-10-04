# Day 29 - End-to-End E-Commerce Project

## Objective

Build an end-to-end e-commerce data processing pipeline using Apache Spark.

The project demonstrates:

- Raw data ingestion
- Data cleaning and transformation
- Pair RDD processing
- Data enrichment using joins
- UDF usage
- Aggregations
- Window functions
- Partitioning
- Persistence
- Spark SQL
- Query execution plan analysis

---

## Architecture

```text
                 Raw E-Commerce Data
                         |
                         v
                  Data Cleaning
                         |
                         v
                 Total Amount
                 Calculation
                         |
             +-----------+-----------+
             |                       |
             v                       v
        Pair RDD                  DataFrame
      Customer Revenue           Processing
             |                       |
             v                       v
       groupByKey()              Product Join
       mapValues()                    |
                                     v
                              Data Enrichment
                                     |
                                     v
                              Customer Segment
                                   UDF
                                     |
                                     v
                              Window Functions
                                     |
                                     v
                              Spark SQL Report
