import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object StreamingAggregationExample {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day24-Streaming-Aggregation")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    println("======================================")
    println("Day 24 - Streaming Aggregation")
    println("======================================")

    val schema = StructType(
      Seq(
        StructField("transaction_id", StringType, true),
        StructField("customer_id", StringType, true),
        StructField("transaction_type", StringType, true),
        StructField("amount", DoubleType, true)
      )
    )

    // --------------------------------------
    // Read streaming transactions
    // --------------------------------------

    val transactions = spark.readStream
      .schema(schema)
      .option("header", "true")
      .csv("data/stream")

    // --------------------------------------
    // Customer-wise aggregation
    // --------------------------------------

    val customerSummary = transactions
      .groupBy("customer_id")
      .agg(
        count("*").alias("transaction_count"),
        sum("amount").alias("total_amount"),
        avg("amount").alias("average_amount"),
        max("amount").alias("maximum_amount"),
        min("amount").alias("minimum_amount")
      )

    // --------------------------------------
    // Console output
    // --------------------------------------

    val query = customerSummary.writeStream
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
