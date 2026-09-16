import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object PairRDDExample {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day 9 Pair RDD")
      .setMaster("local[2]")

    val sc = new SparkContext(conf)

    println("=== Day 9: Pair RDD ===")

    // ---------------------------------------------------------
    // 1. Read sales data
    // ---------------------------------------------------------

    val sales = sc.textFile("data/sales.txt")

    // Create a Pair RDD:
    // (product, revenue)
    val productRevenue = sales.map { line =>
      val parts = line.split(",")
      val product = parts(0)
      val quantity = parts(2).toDouble
      val price = parts(3).toDouble

      (product, quantity * price)
    }

    // ---------------------------------------------------------
    // 2. reduceByKey - Revenue by Product
    // ---------------------------------------------------------

    val revenueByProduct = productRevenue.reduceByKey(_ + _)

    println("\nRevenue by Product using reduceByKey:")
    revenueByProduct.collect().sortBy(_._1).foreach(println)

    // ---------------------------------------------------------
    // 3. groupByKey - Revenue by Product
    // ---------------------------------------------------------

    val groupedProductRevenue = productRevenue
      .groupByKey()
      .mapValues(values => values.sum)

    println("\nRevenue by Product using groupByKey:")
    groupedProductRevenue.collect().sortBy(_._1).foreach(println)

    // ---------------------------------------------------------
    // 4. mapValues - Increase revenue by 10%
    // ---------------------------------------------------------

    val increasedRevenue = revenueByProduct
      .mapValues(revenue => revenue * 1.10)

    println("\nRevenue by Product after 10% increase using mapValues:")
    increasedRevenue.collect().sortBy(_._1).foreach(println)

    // ---------------------------------------------------------
    // 5. Revenue by Department
    // ---------------------------------------------------------

    val departmentRevenue = sales.map { line =>
      val parts = line.split(",")
      val department = parts(1)
      val quantity = parts(2).toDouble
      val price = parts(3).toDouble

      (department, quantity * price)
    }

    val revenueByDepartment = departmentRevenue.reduceByKey(_ + _)

    println("\nRevenue by Department:")
    revenueByDepartment.collect().sortBy(_._1).foreach(println)

    // ---------------------------------------------------------
    // 6. Compare reduceByKey and groupByKey
    // ---------------------------------------------------------

    println("\nPerformance Comparison:")
    println("reduceByKey: Performs local aggregation before shuffle.")
    println("groupByKey: Sends all values for each key before aggregation.")
    println("Therefore, reduceByKey generally transfers less data during shuffle.")

    // ---------------------------------------------------------
    // 7. Bank Transaction Scenario
    // ---------------------------------------------------------

    val bankTransactions = sc.parallelize(
      List(
        ("ACC101", 5000.0),
        ("ACC102", 3000.0),
        ("ACC101", -1000.0),
        ("ACC103", 7000.0),
        ("ACC102", -500.0),
        ("ACC101", 2000.0)
      )
    )

    val balanceByAccount = bankTransactions.reduceByKey(_ + _)

    println("\nBank Balance by Account ID:")
    balanceByAccount.collect().sortBy(_._1).foreach(println)

    // ---------------------------------------------------------
    // 8. Pair RDD Concepts
    // ---------------------------------------------------------

    println("\nPair RDD Operations:")
    println("reduceByKey -> Combines values with the same key")
    println("groupByKey -> Groups all values belonging to the same key")
    println("mapValues  -> Transforms only the values while keeping the keys")

    println("\nPartitions:")
    println(s"Product Revenue Partitions: ${revenueByProduct.getNumPartitions}")
    println(s"Department Revenue Partitions: ${revenueByDepartment.getNumPartitions}")

    sc.stop()
  }
}
