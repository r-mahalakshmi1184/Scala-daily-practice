import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.Trigger

object FileSinkExample {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day27-Streaming-File-Sink")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    println("======================================")
    println("Day 27 - Spark Streaming File Sink")
    println("======================================")

    val schema = StructType(
      Seq(
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

    val query = transactions.writeStream
      .format("csv")
      .outputMode("append")
      .option("path", "output")
      .option("checkpointLocation", "checkpoint")
      .option("header", "true")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()

    println("Streaming query started...")
    println("Waiting for transaction files...")

    query.awaitTermination()
  }
}
