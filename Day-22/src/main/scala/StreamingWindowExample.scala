import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object StreamingWindowExample {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day22-Streaming-Windows")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    println("======================================")
    println("Day 22 - Streaming Aggregations")
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

    val transactions = spark.readStream
      .schema(schema)
      .option("header", "true")
      .csv("data/stream")

    // --------------------------------------
    // Watermark
    // --------------------------------------

    val watermarked = transactions
      .withWatermark("timestamp", "10 minutes")

    // --------------------------------------
    // 10-minute window aggregation
    // --------------------------------------

    val windowedTransactions = watermarked
      .groupBy(
        window(col("timestamp"), "10 minutes"),
        col("transaction_type")
      )
      .agg(
        count("*").alias("transaction_count"),
        sum("amount").alias("total_amount"),
        avg("amount").alias("average_amount")
      )
      .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("transaction_type"),
        col("transaction_count"),
        col("total_amount"),
        col("average_amount")
      )

    // --------------------------------------
    // Console sink
    // --------------------------------------

    val query = windowedTransactions.writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", "false")
      .option("checkpointLocation", "checkpoint")
      .start()

    println("Streaming query started...")
    println("Waiting for incoming transactions...")

    query.awaitTermination()
  }
}
