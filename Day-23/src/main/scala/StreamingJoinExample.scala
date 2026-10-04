import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object StreamingJoinExample {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day23-Streaming-Joins")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    println("======================================")
    println("Day 23 - Spark Streaming Joins")
    println("======================================")

    // --------------------------------------
    // Static Customer Data
    // --------------------------------------

    val customers = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/customers.csv")

    println("\nCustomer Master Data:")
    customers.show()

    // --------------------------------------
    // Streaming Transaction Schema
    // --------------------------------------

    val transactionSchema = StructType(
      Seq(
        StructField("transaction_id", StringType, true),
        StructField("customer_id", StringType, true),
        StructField("transaction_type", StringType, true),
        StructField("amount", DoubleType, true)
      )
    )

    // --------------------------------------
    // Read Streaming Transactions
    // --------------------------------------

    val transactions = spark.readStream
      .schema(transactionSchema)
      .option("header", "true")
      .csv("data/stream")

    // --------------------------------------
    // Streaming + Static Join
    // --------------------------------------

    val enrichedTransactions = transactions
      .join(
        customers,
        transactions("customer_id") === customers("customer_id"),
        "inner"
      )
      .select(
        transactions("transaction_id"),
        transactions("customer_id"),
        customers("name"),
        customers("city"),
        transactions("transaction_type"),
        transactions("amount")
      )

    // --------------------------------------
    // High Value Transactions
    // --------------------------------------

    val highValueTransactions =
      enrichedTransactions
        .filter(col("amount") >= 5000)

    // --------------------------------------
    // Console Output
    // --------------------------------------

    val query = highValueTransactions.writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", "false")
      .option("checkpointLocation", "checkpoint")
      .start()

    println("\nStreaming query started...")
    println("Waiting for transactions...")

    query.awaitTermination()
  }
}

