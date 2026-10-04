import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object KafkaStreamingExample {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day28-Kafka-Streaming")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println("======================================")
    println("Day 28 - Spark Streaming with Kafka")
    println("======================================")

    val kafkaStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "account-transactions")
      .option("startingOffsets", "latest")
      .load()

    val transactions = kafkaStream
      .selectExpr(
        "CAST(key AS STRING) AS key",
        "CAST(value AS STRING) AS value"
      )

    val query = transactions.writeStream
      .format("console")
      .outputMode("append")
      .option("truncate", "false")
      .option("checkpointLocation", "checkpoint")
      .start()

    println("Kafka streaming query started...")
    println("Waiting for Kafka messages...")

    query.awaitTermination()
  }
}
