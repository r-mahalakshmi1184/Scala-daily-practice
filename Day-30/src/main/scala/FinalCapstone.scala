import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.LongAccumulator

object FinalCapstone {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day30-Final-Capstone")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    println("\n==============================================")
    println("DAY 30 - FINAL E-COMMERCE CAPSTONE")
    println("==============================================")

    // --------------------------------------------------
    // 1. READ RAW DATA
    // --------------------------------------------------

    val rawOrders = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/orders.csv")

    val products = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/products.csv")

    println("\n--- RAW ORDERS ---")
    rawOrders.show(false)

    println("\n--- PRODUCTS ---")
    products.show(false)

    // --------------------------------------------------
    // 2. CLEAN DATA
    // --------------------------------------------------

    val cleanOrders = rawOrders
      .filter(col("quantity") > 0 && col("unit_price") > 0)
      .withColumn(
        "total_amount",
        col("quantity") * col("unit_price")
      )
      .persist(StorageLevel.MEMORY_AND_DISK)

    println("\n--- CLEANED ORDERS ---")
    cleanOrders.show(false)

    // --------------------------------------------------
    // 3. PAIR RDD
    // --------------------------------------------------

    val customerRevenueRDD = cleanOrders.rdd
      .map(row => {
        val customerId = row.getAs[String]("customer_id")
        val amount = row.getAs[Int]("total_amount")
        (customerId, amount)
      })
      .groupByKey()
      .mapValues(_.sum)

    println("\n--- PAIR RDD CUSTOMER REVENUE ---")
    customerRevenueRDD
      .sortByKey()
      .collect()
      .foreach(println)

    // --------------------------------------------------
    // 4. ACCUMULATOR
    // --------------------------------------------------

    val validOrderAccumulator: LongAccumulator =
      spark.sparkContext.longAccumulator("ValidOrders")

    cleanOrders.rdd.foreach { row =>
      if (row.getAs[Int]("quantity") > 0) {
        validOrderAccumulator.add(1)
      }
    }

    println(
      s"\nValid orders counted by accumulator: ${validOrderAccumulator.value}"
    )

    // --------------------------------------------------
    // 5. BROADCAST REFERENCE DATA
    // --------------------------------------------------

    val productReference = products
      .select(
        "product_id",
        "product_name",
        "brand",
        "supplier"
      )
      .collect()
      .map(row =>
        row.getAs[String]("product_id") ->
          (
            row.getAs[String]("product_name"),
            row.getAs[String]("brand"),
            row.getAs[String]("supplier")
          )
      )
      .toMap

    val broadcastProducts =
      spark.sparkContext.broadcast(productReference)

    val enrichedOrders = cleanOrders
      .rdd
      .map { row =>

        val productId = row.getAs[String]("product_id")
        val productInfo =
          broadcastProducts.value.get(productId)

        (
          row.getAs[String]("order_id"),
          row.getAs[String]("customer_id"),
          productId,
          row.getAs[String]("category"),
          row.getAs[Int]("quantity"),
          row.getAs[Int]("unit_price"),
          row.getAs[Int]("total_amount"),
          productInfo.map(_._1).getOrElse("Unknown"),
          productInfo.map(_._2).getOrElse("Unknown"),
          productInfo.map(_._3).getOrElse("Unknown")
        )
      }
      .toDF(
        "order_id",
        "customer_id",
        "product_id",
        "category",
        "quantity",
        "unit_price",
        "total_amount",
        "product_name",
        "brand",
        "supplier"
      )

    println("\n--- BROADCAST ENRICHED ORDERS ---")
    enrichedOrders.show(false)

    // --------------------------------------------------
    // 6. PARTITION TUNING
    // --------------------------------------------------

    val partitionedOrders =
      enrichedOrders.repartition(4, col("customer_id"))

    println(
      s"\nPartitions after repartition: ${partitionedOrders.rdd.getNumPartitions}"
    )

    // --------------------------------------------------
    // 7. UDF
    // --------------------------------------------------

    val customerSegment = udf { amount: Int =>
      if (amount >= 50000) "PREMIUM"
      else if (amount >= 20000) "GOLD"
      else "REGULAR"
    }

    val segmentedOrders =
      partitionedOrders.withColumn(
        "customer_segment",
        customerSegment(col("total_amount"))
      )

    println("\n--- UDF CUSTOMER SEGMENTS ---")
    segmentedOrders.show(false)

    // --------------------------------------------------
    // 8. AGGREGATION
    // --------------------------------------------------

    val categorySales = segmentedOrders
      .groupBy("category")
      .agg(
        sum("total_amount").alias("total_sales"),
        count("*").alias("order_count"),
        avg("total_amount").alias("average_order_value")
      )
      .orderBy(desc("total_sales"))

    println("\n--- CATEGORY SALES ---")
    categorySales.show(false)

    // --------------------------------------------------
    // 9. WINDOW FUNCTION
    // --------------------------------------------------

    val customerWindow =
      Window
        .partitionBy("customer_id")
        .orderBy(desc("total_amount"))

    val rankedOrders =
      segmentedOrders.withColumn(
        "customer_order_rank",
        rank().over(customerWindow)
      )

    println("\n--- CUSTOMER ORDER RANKING ---")
    rankedOrders.show(false)

    // --------------------------------------------------
    // 10. SPARK SQL
    // --------------------------------------------------

    rankedOrders.createOrReplaceTempView("ecommerce_orders")

    val sqlReport = spark.sql(
      """
        |SELECT
        |  category,
        |  customer_segment,
        |  COUNT(*) AS orders,
        |  SUM(total_amount) AS sales
        |FROM ecommerce_orders
        |GROUP BY category, customer_segment
        |ORDER BY sales DESC
        |""".stripMargin
    )

    println("\n--- SPARK SQL REPORT ---")
    sqlReport.show(false)

    // --------------------------------------------------
    // 11. EXECUTION PLAN
    // --------------------------------------------------

    println("\n--- PHYSICAL EXECUTION PLAN ---")
    sqlReport.explain(true)

    // --------------------------------------------------
    // 12. FINAL SUMMARY
    // --------------------------------------------------

    println("\n==============================================")
    println("FINAL CAPSTONE SUMMARY")
    println("==============================================")
    println("Pair RDD                : Demonstrated")
    println("Broadcast               : Demonstrated")
    println("Accumulator             : Demonstrated")
    println("Cache / Persist         : Demonstrated")
    println("Partition Tuning        : Demonstrated")
    println("DataFrame               : Demonstrated")
    println("Spark SQL               : Demonstrated")
    println("Aggregation             : Demonstrated")
    println("Window Function         : Demonstrated")
    println("UDF                     : Demonstrated")
    println("Execution Plan          : Demonstrated")
    println("==============================================")

    // Cleanup
    cleanOrders.unpersist()
    broadcastProducts.destroy()

    spark.stop()
  }
}
