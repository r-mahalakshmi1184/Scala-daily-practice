import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkSQLBasics {

  def main(args: Array[String]): Unit = {

    // ---------------------------------------------------------
    // Create Spark Session
    // ---------------------------------------------------------

    val spark = SparkSession.builder()
      .appName("Day 13 Spark SQL Basics")
      .master("local[2]")
      .getOrCreate()

    println("======================================")
    println("Day 13 - Spark SQL Basics")
    println("======================================")

    // ---------------------------------------------------------
    // 1. Read CSV as DataFrame
    // ---------------------------------------------------------

    val customers = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/customers.csv")

    // ---------------------------------------------------------
    // 2. Display DataFrame
    // ---------------------------------------------------------

    println("\nCustomer Data:")
    customers.show()

    // ---------------------------------------------------------
    // 3. Inspect Schema
    // ---------------------------------------------------------

    println("\nCustomer Schema:")
    customers.printSchema()

    // ---------------------------------------------------------
    // 4. Select Columns
    // ---------------------------------------------------------

    println("\nCustomer Names and Cities:")

    customers
      .select("name", "city")
      .show()

    // ---------------------------------------------------------
    // 5. Filter Customers
    // ---------------------------------------------------------

    println("\nCustomers from Chennai:")

    customers
      .filter(col("city") === "Chennai")
      .show()

    // ---------------------------------------------------------
    // 6. Filter High Spending Customers
    // ---------------------------------------------------------

    println("\nCustomers with spending above 50000:")

    customers
      .filter(col("total_spent") > 50000)
      .show()

    // ---------------------------------------------------------
    // 7. withColumn
    // ---------------------------------------------------------

    val customersWithCategory = customers.withColumn(
      "customer_category",
      when(col("total_spent") >= 70000, "Premium")
        .when(col("total_spent") >= 50000, "Gold")
        .otherwise("Regular")
    )

    println("\nCustomers with Category:")

    customersWithCategory.show()

    // ---------------------------------------------------------
    // 8. Create Temporary View
    // ---------------------------------------------------------

    customersWithCategory.createOrReplaceTempView("customers")

    // ---------------------------------------------------------
    // 9. SQL Query
    // ---------------------------------------------------------

    println("\nSQL - Chennai Customers:")

    spark.sql(
      """
        SELECT customer_id, name, city, total_spent
        FROM customers
        WHERE city = 'Chennai'
      """
    ).show()

    // ---------------------------------------------------------
    // 10. SQL - High Value Customers
    // ---------------------------------------------------------

    println("\nSQL - High Value Customers:")

    spark.sql(
      """
        SELECT customer_id, name, total_spent, customer_category
        FROM customers
        WHERE total_spent > 50000
        ORDER BY total_spent DESC
      """
    ).show()

    // ---------------------------------------------------------
    // 11. Customer Analytics Report
    // ---------------------------------------------------------

    println("\n======================================")
    println("Customer Analytics Report")
    println("======================================")

    spark.sql(
      """
        SELECT
          city,
          COUNT(*) AS customer_count,
          ROUND(AVG(total_spent), 2) AS average_spending,
          SUM(total_spent) AS total_city_spending
        FROM customers
        GROUP BY city
        ORDER BY total_city_spending DESC
      """
    ).show()

    // ---------------------------------------------------------
    // 12. Stop Spark
    // ---------------------------------------------------------

    spark.stop()
  }
}
