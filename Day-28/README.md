cat > README.md <<'EOF'
# Day 28 - Real-Time Booking Project

## Overview

This project simulates a real-time booking system using Apache Spark.

The system processes booking and cancellation events for:

- Flights
- Hotels
- Buses

It demonstrates Pair RDD operations, booking state management, broadcast variables, Spark SQL, and window functions.

## Objectives

- Process booking and cancellation events
- Maintain active booking state
- Calculate occupancy and availability
- Use Pair RDD for booking event counts
- Use broadcast reference data for capacity
- Generate reports using Spark SQL
- Use window functions for booking ranking

## Technologies Used

- Scala 2.12.18
- Apache Spark 3.5.3
- Spark SQL
- Spark Core
- SBT
- Ubuntu / WSL

## Project Structure

```text
Day-28/
├── build.sbt
├── data/
│   └── bookings.csv
├── src/
│   └── main/
│       └── scala/
│           ├── KafkaStreamingExample.scala
│           └── RealTimeBookingProject.scala
└── README.md
