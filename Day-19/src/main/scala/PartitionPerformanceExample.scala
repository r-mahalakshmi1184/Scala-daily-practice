import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object PartitionPerformanceExample {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day19-Partitioning-Performance")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    println("======================================")
    println("Day 19 - Partitioning & Performance")
    println("======================================")

    val sales = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/sales.csv")

    println("\nOriginal Data:")
    sales.show()

    // 1. Inspect partitions
    println("--------------------------------------")
    println("1. Original Partition Count")
    println("--------------------------------------")

    println(s"Partitions: ${sales.rdd.getNumPartitions}")

    // 2. Repartition
    val repartitioned = sales.repartition(4)

    println("\n--------------------------------------")
    println("2. After Repartition")
    println("--------------------------------------")

    println(s"Partitions: ${repartitioned.rdd.getNumPartitions}")

    // 3. Coalesce
    val coalesced = repartitioned.coalesce(2)

    println("\n--------------------------------------")
    println("3. After Coalesce")
    println("--------------------------------------")

    println(s"Partitions: ${coalesced.rdd.getNumPartitions}")

    // 4. Partition by category
    val partitionedByCategory =
      sales.rdd
        .map(row => (row.getAs[String]("category"), row))
        .partitionBy(
          new org.apache.spark.HashPartitioner(2)
        )

    println("\n--------------------------------------")
    println("4. Pair RDD partitionBy")
    println("--------------------------------------")

    println(
      s"Partitions: ${partitionedByCategory.getNumPartitions}"
    )

    // 5. Partition distribution
    println("\n--------------------------------------")
    println("5. Records Per Partition")
    println("--------------------------------------")

    val distribution = repartitioned.rdd
      .mapPartitions(iter => Iterator(iter.size))
      .collect()

    distribution.zipWithIndex.foreach {
      case (count, index) =>
        println(s"Partition $index -> $count records")
    }

    // 6. Scenario
    println("\n--------------------------------------")
    println("6. Scenario - Too Few Partitions")
    println("--------------------------------------")

    println("If a dataset has too few partitions:")
    println("1. Increase partitions using repartition().")
    println("2. More partitions allow more parallel tasks.")
    println("3. Avoid creating excessive partitions.")
    println("4. Use coalesce() when reducing partitions.")

    println("\n======================================")
    println("Day 19 Completed Successfully")
    println("======================================")

    spark.stop()
  }
}
