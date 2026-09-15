import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object DAGSparkExecution {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day 8 DAG and Spark Execution")
      .setMaster("local[2]")

    val sc = new SparkContext(conf)

    println("=== Day 8: DAG and Spark Execution ===")

    // Step 1: Read sales data
    val sales = sc.textFile("data/sales.txt")

    // Step 2: Convert each line into (product, amount)
    val productSales = sales.map { line =>
      val parts = line.split(",")
      (parts(0), parts(2).toDouble)
    }

    // Step 3: Keep sales greater than 5000
    val filteredSales = productSales.filter {
      case (_, amount) => amount > 5000
    }

    // Step 4: Aggregate revenue by product
    // reduceByKey creates a shuffle boundary
    val revenueByProduct = filteredSales.reduceByKey(_ + _)

    // Action 1: Count the records
    println(s"\nNumber of sales records: ${sales.count()}")

    // Action 2: Display the first product revenue
    println(s"\nFirst product revenue: ${revenueByProduct.first()}")

    // Action 3: Display all product revenues
    println("\nRevenue by Product:")
    revenueByProduct.collect().foreach(println)

    // Display partition information
    println(s"\nNumber of Partitions: ${revenueByProduct.getNumPartitions}")

    // Display RDD lineage / DAG information
    println("\nRDD Lineage / DAG:")
    println(revenueByProduct.toDebugString)

    // Explain the transformations
    println("\nTransformation Types:")
    println("map -> Narrow transformation")
    println("filter -> Narrow transformation")
    println("reduceByKey -> Wide transformation")
    println("reduceByKey causes a shuffle boundary")

    // Explain Spark execution
    println("\nSpark Execution Concepts:")
    println("Job = created when an action is executed")
    println("Stage = group of tasks separated by shuffle boundaries")
    println("Task = work performed on one partition")
    println("Partition = portion of the dataset processed in parallel")

    // Predict stages for this pipeline
println("\nStage and Shuffle Analysis:")
println("Stage 1: textFile -> map -> filter")
println("Shuffle Boundary: reduceByKey")
println("Stage 2: reduceByKey")
println("Total Predicted Stages: 2")

println("\nDAG Structure:")
println("textFile -> map -> filter -> SHUFFLE -> reduceByKey")
    sc.stop()
  }
}
