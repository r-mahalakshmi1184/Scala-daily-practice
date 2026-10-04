# Day 19 – Broadcast Join

## Overview

This practice focuses on optimizing Spark joins using Broadcast Join when one dataset is significantly smaller than the other.

## Objectives

- Create a large fact DataFrame
- Create a small reference DataFrame
- Implement a Broadcast Join
- Understand when Broadcast Join is appropriate
- Compare Broadcast Join with Shuffle Sort Merge Join

## Scenario

Join a large transaction dataset containing millions of records with a small branch master/reference dataset.

The smaller reference dataset can be broadcast to the executors to avoid unnecessary shuffle of the large dataset.

## Key Concepts

- Broadcast Variables
- Broadcast Join
- Fact Data
- Reference Data
- Shuffle
- Shuffle Sort Merge Join
- Join Optimization

## Performance Consideration

Broadcast Join can reduce shuffle when the smaller dataset can safely fit within the executor memory.

It should be used carefully because broadcasting a dataset that is too large can increase memory usage and potentially cause executor memory problems.

## Learning Outcome

Learned how Broadcast Join can improve join performance when joining a large dataset with a small reference dataset.

## Technologies

- Scala
- Apache Spark
- Spark SQL
- SBT
