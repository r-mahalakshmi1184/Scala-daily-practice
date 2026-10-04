# Day 22 – Batch Mini Project

## Overview

This practice implements an end-to-end batch data processing pipeline using Apache Spark.

## Project Scenario

### E-Commerce Daily Sales Pipeline

The pipeline processes raw transaction data and generates aggregated sales information.

## Pipeline

```text
Raw Transactions
       ↓
Data Cleaning
       ↓
Customer/Product Join
       ↓
Revenue Aggregation
       ↓
Partitioned Parquet Output
