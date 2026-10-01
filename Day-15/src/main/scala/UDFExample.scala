import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object UDFExample {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day 15 UDF Practice")
      .master("local[2]")
      .getOrCreate()

    println("======================================")
    println("Day 15 - UDF Practice")
    println("======================================")

    // ---------------------------------------------------------
    // 1. Read customer data
    // ---------------------------------------------------------

    val customers = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/customers.csv")

    println("\nCustomer Data:")
    customers.show()

    // ---------------------------------------------------------
    // 2. Create Scala UDF for salary band
    // ---------------------------------------------------------

    val classifySalary = udf((amount: Double) => {

      if (amount >= 100000) {
        "High"
      } else if (amount >= 50000) {
        "Medium"
      } else {
        "Low"
      }

    })

    // ---------------------------------------------------------
    // 3. Add salary/risk classification column
    // ---------------------------------------------------------

    val classifiedCustomers = customers.withColumn(
      "salary_band",
      classifySalary(col("total_amount"))
    )

    println("\nCustomers with Salary Band:")

    classifiedCustomers.show()

    // ---------------------------------------------------------
    // 4. Built-in Spark function comparison
    // ---------------------------------------------------------

    val builtInClassification = customers.withColumn(
      "amount_category",
      when(col("total_amount") >= 100000, "High")
        .when(col("total_amount") >= 50000, "Medium")
        .otherwise("Low")
    )

    println("\nBuilt-in Spark Function Result:")

    builtInClassification.show()

    // ---------------------------------------------------------
    // 5. Register UDF with Spark SQL
    // ---------------------------------------------------------

    spark.udf.register(
      "classify_transaction_risk",
      (amount: Double) => {

        if (amount >= 100000) {
          "High Risk"
        } else if (amount >= 50000) {
          "Medium Risk"
        } else {
          "Low Risk"
        }

      }
    )

    // ---------------------------------------------------------
    // 6. Create temporary view
    // ---------------------------------------------------------

    customers.createOrReplaceTempView("customers")

    // ---------------------------------------------------------
    // 7. Use registered UDF in SQL
    // ---------------------------------------------------------

    println("\nCustomer Risk Classification using SQL UDF:")

    spark.sql(
      """
        SELECT
          customer_id,
          name,
          total_transactions,
          total_amount,
          classify_transaction_risk(total_amount)
            AS risk_category
        FROM customers
        ORDER BY total_amount DESC
      """
    ).show()

    // ---------------------------------------------------------
    // 8. Customer Risk Report
    // ---------------------------------------------------------

    println("\n======================================")
    println("Customer Risk Report")
    println("======================================")

    spark.sql(
      """
        SELECT
          classify_transaction_risk(total_amount) AS risk_category,
          COUNT(*) AS customer_count,
          SUM(total_amount) AS total_amount
        FROM customers
        GROUP BY classify_transaction_risk(total_amount)
        ORDER BY total_amount DESC
      """
    ).show()

    // ---------------------------------------------------------
    // 9. UDF vs Built-in Function
    // ---------------------------------------------------------

    println("\n======================================")
    println("UDF vs Built-in Function")
    println("======================================")

    println("UDF:")
    println("- Allows custom business logic.")
    println("- Useful when Spark does not provide the required function.")
    println("- Can have additional serialization and execution overhead.")

    println("\nBuilt-in Spark Function:")
    println("- Usually preferred when available.")
    println("- Spark can optimize built-in expressions better.")
    println("- Examples: when(), sum(), avg(), upper(), lower().")

    println("\n======================================")
    println("Day 15 Completed Successfully")
    println("======================================")

    spark.stop()
  }
}
