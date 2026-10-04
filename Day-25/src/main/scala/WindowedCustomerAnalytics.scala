import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object WindowedCustomerAnalytics {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day25-Windowed-Customer-Analytics")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    println("======================================")
    println("Day 25 - Windowed Customer Analytics")
    println("======================================")

    val schema = StructType(
      Seq(
        StructField("timestamp", TimestampType, true),
        StructField("transaction_id", StringType, true),
        StructField("customer_id", StringType, true),
        StructField("transaction_type", StringType, true),
        StructField("amount", DoubleType, true)
      )
    )

    // Read streaming transactions
    val transactions = spark.readStream
      .schema(schema)
      .option("header", "true")
      .csv("data/stream")

    // Add watermark for late-arriving data
    val watermarkedTransactions = transactions
      .withWatermark("timestamp", "10 minutes")

    // Customer analytics using a 10-minute window
    val customerAnalytics = watermarkedTransactions
      .groupBy(
        window(col("timestamp"), "10 minutes"),
        col("customer_id")
      )
      .agg(
        count("*").alias("transaction_count"),
        sum("amount").alias("total_amount"),
        avg("amount").alias("average_amount")
      )
      .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("customer_id"),
        col("transaction_count"),
        col("total_amount"),
        col("average_amount")
      )

    val query = customerAnalytics.writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", "false")
      .option("checkpointLocation", "checkpoint")
      .start()

    println("Streaming query started...")
    println("Waiting for transaction files...")

    query.awaitTermination()
  }
}
