# Day 17 — Spark Joins

## Objective

Practice different types of joins using Spark DataFrames and create a customer revenue report.

## Tasks Completed

- Created customer and order DataFrames
- Practiced Inner Join
- Practiced Left Join
- Practiced Right Join
- Aggregated customer revenue
- Calculated order count
- Created a customer revenue report

## Datasets

### customers.csv

Columns:

- customer_id
- name
- city

### orders.csv

Columns:

- order_id
- customer_id
- product
- amount

## Join Types

### Inner Join

Returns records that have matching customer IDs in both DataFrames.

```scala
customers.join(
  orders,
  customers("customer_id") === orders("customer_id"),
  "inner"
)
