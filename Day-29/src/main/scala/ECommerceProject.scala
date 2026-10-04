import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.storage.StorageLevel

object ECommerceProject {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day29-End-to-End-ECommerce")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println("==============================================")
    println("Day 29 - End-to-End E-Commerce Project")
    println("==============================================")

    // --------------------------------------------------
    // 1. RAW DATA
    // --------------------------------------------------

    val rawOrders = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/ecommerce.csv")

    val products = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/products.csv")

    println("\n--- RAW ORDERS ---")
    rawOrders.show(false)

    println("\n--- PRODUCT REFERENCE DATA ---")
    products.show(false)

    // --------------------------------------------------
    // 2. CLEAN
    // --------------------------------------------------

    val cleanOrders = rawOrders
      .filter(
        col("order_id").isNotNull &&
        col("customer_id").isNotNull &&
        col("product_id").isNotNull &&
        col("quantity") > 0 &&
        col("unit_price") > 0
      )
      .withColumn(
        "order_date",
        to_date(col("order_date"))
      )
      .withColumn(
        "total_amount",
        col("quantity") * col("unit_price")
      )

    println("\n--- CLEANED ORDERS ---")
    cleanOrders.show(false)

    // --------------------------------------------------
    // 3. RDD / PAIR RDD COMPONENT
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
    // 4. PERSIST CLEAN DATA
    // --------------------------------------------------

    cleanOrders.persist(StorageLevel.MEMORY_AND_DISK)

    println("\n--- PERSISTENCE ---")
    println("Clean orders persisted using MEMORY_AND_DISK")

    // --------------------------------------------------
    // 5. ENRICH USING JOIN
    // --------------------------------------------------

    val enrichedOrders = cleanOrders
      .join(
        products,
        cleanOrders.col("product_id") === products.col("product_id")
      )
      .select(
        cleanOrders.col("order_id"),
        cleanOrders.col("customer_id"),
        cleanOrders.col("product_id"),
        products.col("product_name"),
        products.col("brand"),
        products.col("category"),
        products.col("supplier"),
        cleanOrders.col("quantity"),
        cleanOrders.col("unit_price"),
        cleanOrders.col("total_amount"),
        cleanOrders.col("order_date")
      )

    println("\n--- ENRICHED ORDERS ---")
    enrichedOrders.show(false)

    // --------------------------------------------------
    // 6. REPARTITION
    // --------------------------------------------------

    val partitionedOrders =
      enrichedOrders.repartition(4, col("customer_id"))

    println("\n--- PARTITION INFORMATION ---")
    println(
      s"Number of partitions: ${partitionedOrders.rdd.getNumPartitions}"
    )

    // --------------------------------------------------
    // 7. UDF
    // --------------------------------------------------

    val customerSegment = udf((amount: Double) => {

      if (amount >= 50000)
        "PREMIUM"
      else if (amount >= 20000)
        "GOLD"
      else
        "REGULAR"

    })

    val segmentedOrders = partitionedOrders
      .withColumn(
        "customer_segment",
        customerSegment(col("total_amount").cast("double"))
      )

    println("\n--- CUSTOMER SEGMENT ---")
    segmentedOrders.show(false)

    // --------------------------------------------------
    // 8. AGGREGATION
    // --------------------------------------------------

    val categorySales = segmentedOrders
      .groupBy("category")
      .agg(
        sum("total_amount").alias("total_sales"),
        count("order_id").alias("order_count"),
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
        .orderBy(
          col("total_amount").desc
        )

    val rankedOrders = segmentedOrders
      .withColumn(
        "customer_order_rank",
        rank().over(customerWindow)
      )

    println("\n--- CUSTOMER ORDER RANKING ---")
    rankedOrders.show(false)

    // --------------------------------------------------
    // 10. SPARK SQL
    // --------------------------------------------------

    segmentedOrders.createOrReplaceTempView("ecommerce_orders")

    val sqlReport = spark.sql(
      """
        SELECT
          category,
          customer_segment,
          COUNT(*) AS orders,
          SUM(total_amount) AS revenue
        FROM ecommerce_orders
        GROUP BY category, customer_segment
        ORDER BY revenue DESC
      """
    )

    println("\n--- SPARK SQL REPORT ---")
    sqlReport.show(false)

    // --------------------------------------------------
    // 11. EXPLAIN PLAN
    // --------------------------------------------------

    println("\n--- EXECUTION PLAN ---")
    categorySales.explain(true)

    println("\n==============================================")
    println("Day 29 processing completed successfully")
    println("==============================================")

    cleanOrders.unpersist()

    spark.stop()
  }
}
