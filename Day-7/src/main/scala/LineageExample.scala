import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object LineageExample {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day 7 Immutability Lineage Fault Tolerance")
      .setMaster("local[2]")

    val sc = new SparkContext(conf)

    println("=== Day 7: Immutability, Lineage and Fault Tolerance ===")

    // Step 1: Read numbers from the text file
    val numbers = sc.textFile("data/numbers.txt")

    // Step 2: Convert strings to integers
    val integers = numbers.map(_.toInt)

    // Step 3: Keep only even numbers
    val evenNumbers = integers.filter(_ % 2 == 0)

    // Step 4: Multiply each number by 10
    val multiplied = evenNumbers.map(_ * 10)

    // Step 5: Keep values greater than 20
    val finalRDD = multiplied.filter(_ > 20)

    // Demonstrate RDD immutability
    println("\nImmutability Demonstration:")
    println(s"Original numbers: ${numbers.collect().mkString(", ")}")
    println(s"Even numbers (new RDD): ${evenNumbers.collect().mkString(", ")}")
    println(s"Multiplied values (new RDD): ${multiplied.collect().mkString(", ")}")

    // Display the final result
    println("\nFinal Result:")
    finalRDD.collect().foreach(println)

    // Display the number of partitions
    println(s"\nNumber of Partitions: ${finalRDD.getNumPartitions}")

    // Display the RDD lineage
    println("\nRDD Lineage:")
    println(finalRDD.toDebugString)

    // Fault tolerance simulation
    println("\nFault Tolerance Simulation:")
    println("If a partition of finalRDD is lost, Spark can recompute it")
    println("by following the RDD lineage from the original data.")
    println("Original data -> map -> filter -> map -> filter -> finalRDD")

    sc.stop()
  }
}
