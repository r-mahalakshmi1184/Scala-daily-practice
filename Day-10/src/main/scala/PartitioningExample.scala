import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.HashPartitioner

object PartitioningExample {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day 10 Partitioning")
      .setMaster("local[2]")

    val sc = new SparkContext(conf)

    println("=== Day 10: Partitioning ===")

    // ---------------------------------------------------------
    // 1. Create an RDD with too few partitions
    // ---------------------------------------------------------

    val customers = sc.textFile("data/customers.txt", 1)

    println("\nInitial Partition Count:")
    println(customers.getNumPartitions)

    // ---------------------------------------------------------
    // 2. Repartition
    // ---------------------------------------------------------

    val repartitionedCustomers = customers.repartition(4)

    println("\nAfter repartition(4):")
    println(repartitionedCustomers.getNumPartitions)

    // ---------------------------------------------------------
    // 3. Coalesce
    // ---------------------------------------------------------

    val coalescedCustomers = repartitionedCustomers.coalesce(2)

    println("\nAfter coalesce(2):")
    println(coalescedCustomers.getNumPartitions)

    // ---------------------------------------------------------
    // 4. Display customer records
    // ---------------------------------------------------------

    println("\nCustomer Records:")
    coalescedCustomers.collect().foreach(println)

    // ---------------------------------------------------------
    // 5. Create a Pair RDD
    // ---------------------------------------------------------

    val customerByCity = customers.map { line =>
      val parts = line.split(",")
      (parts(2), parts(0))
    }

    println("\nPair RDD Before partitionBy:")
    println(s"Partitions: ${customerByCity.getNumPartitions}")

    // ---------------------------------------------------------
    // 6. Use partitionBy on Pair RDD
    // ---------------------------------------------------------

    val partitionedByCity = customerByCity.partitionBy(
      new HashPartitioner(3)
    )

    println("\nPair RDD After partitionBy(3):")
    println(s"Partitions: ${partitionedByCity.getNumPartitions}")

    println("\nCustomers Partitioned by City:")
    partitionedByCity.collect().sortBy(_._1).foreach(println)

    // ---------------------------------------------------------
    // 7. Explain partition operations
    // ---------------------------------------------------------

    println("\nPartitioning Concepts:")

    println("\nrepartition:")
    println("Used to increase or decrease the number of partitions.")
    println("It causes a full shuffle of the data.")

    println("\ncoalesce:")
    println("Used mainly to decrease the number of partitions.")
    println("It can avoid a full shuffle when reducing partitions.")

    println("\npartitionBy:")
    println("Used with Pair RDDs to control how keys are distributed.")
    println("HashPartitioner places the same keys into the same partition.")

    // ---------------------------------------------------------
    // 8. Optimization scenario
    // ---------------------------------------------------------

    println("\nOptimization Scenario:")
    println("Problem: Dataset has too few partitions.")
    println("Solution: Increase partitions using repartition().")
    println("More partitions can provide more parallelism.")
    println("However, too many partitions can create scheduling overhead.")

    sc.stop()
  }
}
