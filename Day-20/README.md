# Day 20 – File Formats and Output

## Overview

This practice focuses on reading and writing different file formats using Apache Spark and understanding how Spark stores distributed output.

## Objectives

- Read CSV files
- Read JSON files
- Read Parquet files
- Write CSV output
- Write JSON output
- Write Parquet output
- Write partitioned output
- Understand Spark file layout
- Understand the number of output files
- Practice repartitioning before writing

## Scenario

Store daily sales data using a partitioned structure based on:

- Year
- Month
- Day

Example:

```text
output/
├── year=2026/
│   ├── month=10/
│   │   ├── day=01/
│   │   └── day=02/
