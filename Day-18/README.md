# Day 18 – Spark Joins

## Overview

This practice focuses on joining multiple datasets using Apache Spark DataFrames and understanding how different join strategies affect data processing.

## Objectives

- Implement Inner Join
- Implement Left Join
- Implement Right Join
- Implement Full Outer Join
- Handle ambiguous column names using aliases
- Handle null values after joins
- Understand Shuffle Sort Merge Join

## Scenario

Join related datasets such as:

- Orders
- Customers
- Payments

The objective is to combine information from multiple sources and produce a meaningful business-level dataset.

## Key Concepts

- DataFrame Joins
- Join Conditions
- Aliases
- Null Handling
- Shuffle
- Shuffle Sort Merge Join
- Distributed Data Processing

## Learning Outcome

Learned how to combine multiple DataFrames using different join types and how Spark performs joins internally.

Also understood that joins can introduce shuffle operations and therefore may have a significant impact on Spark job performance.

## Technologies

- Scala
- Apache Spark
- Spark SQL
- SBT
