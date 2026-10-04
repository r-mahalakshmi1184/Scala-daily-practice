import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object PerformanceOptimizationExample {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day20-Performance-Optimization")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    println("======================================")
    println("Day 20 - Spark Performance Optimization")
    println("======================================")

    val sales = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/sales.csv")

    println("\nOriginal Data:")
    sales.show()

    // --------------------------------------
    // 1. Inspect the query plan
    // --------------------------------------

    println("--------------------------------------")
    println("1. Query Plan")
    println("--------------------------------------")

    sales
      .filter(col("amount") > 10000)
      .select("customer_id", "product", "amount")
      .explain(true)

    // --------------------------------------
    // 2. Filter early
    // --------------------------------------

    println("\n--------------------------------------")
    println("2. Filter Before Aggregation")
    println("--------------------------------------")

    val filteredSales = sales
      .filter(col("amount") > 10000)

    filteredSales.show()

    // --------------------------------------
    // 3. Select only required columns
    // --------------------------------------

    println("\n--------------------------------------")
    println("3. Select Required Columns")
    println("--------------------------------------")

    val selectedSales = sales
      .select("customer_id", "category", "amount")

    selectedSales.show()

    // --------------------------------------
    // 4. Aggregation
    // --------------------------------------

    println("\n--------------------------------------")
    println("4. Category Revenue")
    println("--------------------------------------")

    val categoryRevenue = sales
      .groupBy("category")
      .agg(
        sum("amount").alias("total_revenue"),
        avg("amount").alias("average_amount"),
        count("*").alias("transaction_count")
      )

    categoryRevenue.show()

    // --------------------------------------
    // 5. Repartition
    // --------------------------------------

    println("\n--------------------------------------")
    println("5. Repartition")
    println("--------------------------------------")

    println(
      s"Original partitions: ${sales.rdd.getNumPartitions}"
    )

    val repartitioned = sales.repartition(4)

    println(
      s"After repartition: ${repartitioned.rdd.getNumPartitions}"
    )

    // --------------------------------------
    // 6. Coalesce
    // --------------------------------------

    println("\n--------------------------------------")
    println("6. Coalesce")
    println("--------------------------------------")

    val coalesced = repartitioned.coalesce(2)

    println(
      s"After coalesce: ${coalesced.rdd.getNumPartitions}"
    )

    // --------------------------------------
    // 7. Broadcast variable
    // --------------------------------------

    println("\n--------------------------------------")
    println("7. Broadcast Example")
    println("--------------------------------------")

    val categoryInfo = Map(
      "Electronics" -> "Electronic Products",
      "Furniture" -> "Home Furniture"
    )

    val broadcastCategoryInfo =
      spark.sparkContext.broadcast(categoryInfo)

    val enrichedSales = sales
      .rdd
      .map { row =>
        val category =
          row.getAs[String]("category")

        val description =
          broadcastCategoryInfo.value
            .getOrElse(category, "Unknown")

        (
          row.getAs[String]("product"),
          category,
          description,
          row.getAs[Int]("amount")
        )
      }

    enrichedSales.collect().foreach(println)

    // --------------------------------------
    // 8. Optimization scenario
    // --------------------------------------

    println("\n--------------------------------------")
    println("8. Optimization Scenario")
    println("--------------------------------------")

    println("Optimization techniques used:")
    println("1. Filter unnecessary records early.")
    println("2. Select only required columns.")
    println("3. Use appropriate partition counts.")
    println("4. Use coalesce when reducing partitions.")
    println("5. Use broadcast for small reference data.")
    println("6. Inspect query plans using explain().")

    println("\n======================================")
    println("Day 20 Completed Successfully")
    println("======================================")

    spark.stop()
  }
}
