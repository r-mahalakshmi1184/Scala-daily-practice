import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object StructuredStreamingExample {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day21-Structured-Streaming")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    println("======================================")
    println("Day 21 - Spark Structured Streaming")
    println("======================================")

    val schema = StructType(
      Seq(
        StructField("transaction_id", StringType, true),
        StructField("customer_id", StringType, true),
        StructField("transaction_type", StringType, true),
        StructField("amount", DoubleType, true)
      )
    )

    // Read CSV files as a streaming source
    val transactions = spark.readStream
      .schema(schema)
      .option("header", "true")
      .csv("data/stream")

    // --------------------------------------
    // Transaction Summary
    // --------------------------------------

    val summary = transactions
      .groupBy("transaction_type")
      .agg(
        count("*").alias("transaction_count"),
        sum("amount").alias("total_amount"),
        avg("amount").alias("average_amount")
      )

    // --------------------------------------
    // Console Output
    // --------------------------------------

    val query = summary.writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", "false")
      .option("checkpointLocation", "checkpoint")
      .start()

    println("Streaming query started...")
    println("Waiting for incoming transaction files...")

    query.awaitTermination()
  }
}
