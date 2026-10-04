import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object CheckpointExample {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day26-Streaming-Checkpointing")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    println("======================================")
    println("Day 26 - Streaming Checkpointing")
    println("======================================")

    val schema = StructType(
      Seq(
        StructField("transaction_id", StringType, true),
        StructField("customer_id", StringType, true),
        StructField("transaction_type", StringType, true),
        StructField("amount", DoubleType, true)
      )
    )

    // Read streaming data
    val transactions = spark.readStream
      .schema(schema)
      .option("header", "true")
      .csv("data/stream")

    // Customer-wise aggregation
    val customerSummary = transactions
      .groupBy("customer_id")
      .agg(
        count("*").alias("transaction_count"),
        sum("amount").alias("total_amount"),
        avg("amount").alias("average_amount")
      )

    // Checkpoint location
    val query = customerSummary.writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", "false")
      .option("checkpointLocation", "checkpoint")
      .start()

    println("Streaming query started...")
    println("Checkpoint location: checkpoint")
    println("Waiting for transaction files...")

    query.awaitTermination()
  }
}
