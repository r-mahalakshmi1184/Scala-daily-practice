import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkJoinsExample {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day 17 Spark Joins")
      .master("local[2]")
      .getOrCreate()

    println("======================================")
    println("Day 17 - Spark Joins")
    println("======================================")

    // Read customers
    val customers = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/customers.csv")

    // Read orders
    val orders = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/orders.csv")

    println("\nCustomers:")
    customers.show()

    println("\nOrders:")
    orders.show()

    // ---------------------------------------------------------
    // INNER JOIN
    // ---------------------------------------------------------

    println("\n======================================")
    println("INNER JOIN")
    println("======================================")

    val innerJoin = customers.join(
      orders,
      customers("customer_id") === orders("customer_id"),
      "inner"
    )

    innerJoin
      .select(
        customers("customer_id"),
        customers("name"),
        customers("city"),
        orders("product"),
        orders("amount")
      )
      .show()

    // ---------------------------------------------------------
    // LEFT JOIN
    // ---------------------------------------------------------

    println("\n======================================")
    println("LEFT JOIN")
    println("======================================")

    val leftJoin = customers.join(
      orders,
      customers("customer_id") === orders("customer_id"),
      "left"
    )

    leftJoin
      .select(
        customers("customer_id"),
        customers("name"),
        orders("product"),
        orders("amount")
      )
      .show()

    // ---------------------------------------------------------
    // RIGHT JOIN
    // ---------------------------------------------------------

    println("\n======================================")
    println("RIGHT JOIN")
    println("======================================")

    val rightJoin = customers.join(
      orders,
      customers("customer_id") === orders("customer_id"),
      "right"
    )

    rightJoin
      .select(
        customers("name"),
        orders("order_id"),
        orders("product"),
        orders("amount")
      )
      .show()

    // ---------------------------------------------------------
    // CUSTOMER REVENUE
    // ---------------------------------------------------------

    println("\n======================================")
    println("Customer Revenue Report")
    println("======================================")

    customers
      .join(
        orders,
        customers("customer_id") === orders("customer_id"),
        "inner"
      )
      .groupBy(
        customers("customer_id"),
        customers("name")
      )
      .agg(
        sum(orders("amount")).alias("total_revenue"),
        count(orders("order_id")).alias("order_count")
      )
      .orderBy(desc("total_revenue"))
      .show()

    println("\n======================================")
    println("Day 17 Completed Successfully")
    println("======================================")

    spark.stop()
  }
}
