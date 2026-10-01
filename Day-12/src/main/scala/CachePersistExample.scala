import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel

object CachePersistExample {

  def main(args: Array[String]): Unit = {

    // ---------------------------------------------------------
    // Spark Configuration
    // ---------------------------------------------------------

    val conf = new SparkConf()
      .setAppName("Day 12 Cache and Persist")
      .setMaster("local[2]")

    val sc = new SparkContext(conf)

    println("======================================")
    println("Day 12 - Cache and Persist")
    println("======================================")

    // ---------------------------------------------------------
    // 1. Read transaction data
    // ---------------------------------------------------------

    val transactions = sc.textFile("data/transactions.txt")

    println("\nOriginal Transaction Count:")
    println(transactions.count())

    // ---------------------------------------------------------
    // 2. Clean the transaction data
    // ---------------------------------------------------------

    val cleanedTransactions = transactions
      .map(_.trim)
      .filter(_.nonEmpty)
      .filter(_.split(",").length == 4)

    // ---------------------------------------------------------
    // 3. Cache the cleaned RDD
    // ---------------------------------------------------------

    cleanedTransactions.cache()

    println("\nCleaned Transaction Count:")
    println(cleanedTransactions.count())

    // ---------------------------------------------------------
    // 4. Report 1 - Total Transactions
    // ---------------------------------------------------------

    val totalTransactions = cleanedTransactions.count()

    println("\n--------------------------------------")
    println("Report 1 - Total Transactions")
    println("--------------------------------------")
    println(s"Total Transactions: $totalTransactions")

    // ---------------------------------------------------------
    // 5. Report 2 - Electronics Transactions
    // ---------------------------------------------------------

    val electronicsCount = cleanedTransactions
      .filter { line =>
        val parts = line.split(",")
        parts(2) == "Electronics"
      }
      .count()

    println("\n--------------------------------------")
    println("Report 2 - Electronics Transactions")
    println("--------------------------------------")
    println(s"Electronics Transactions: $electronicsCount")

    // ---------------------------------------------------------
    // 6. Report 3 - Total Transaction Amount
    // ---------------------------------------------------------

    val totalAmount = cleanedTransactions
      .map { line =>
        val parts = line.split(",")
        parts(3).toDouble
      }
      .sum()

    println("\n--------------------------------------")
    println("Report 3 - Total Transaction Amount")
    println("--------------------------------------")
    println(s"Total Transaction Amount: $totalAmount")

    // ---------------------------------------------------------
    // 7. Demonstrate Persist
    // ---------------------------------------------------------

    // Remove the cached RDD
    cleanedTransactions.unpersist()

    // Create another cleaned RDD
    val persistedTransactions = transactions
      .map(_.trim)
      .filter(_.nonEmpty)
      .filter(_.split(",").length == 4)

    // Persist using MEMORY_ONLY
    persistedTransactions.persist(StorageLevel.MEMORY_ONLY)

    println("\n--------------------------------------")
    println("Persist Example")
    println("--------------------------------------")

    println(s"Persisted Transaction Count: ${persistedTransactions.count()}")

    println("\nStorage Level:")
    println(persistedTransactions.getStorageLevel)

    // ---------------------------------------------------------
    // 8. Reuse persisted RDD
    // ---------------------------------------------------------

    val furnitureCount = persistedTransactions
      .filter { line =>
        val parts = line.split(",")
        parts(2) == "Furniture"
      }
      .count()

    println("\nFurniture Transactions:")
    println(furnitureCount)

    // ---------------------------------------------------------
    // 9. Cache vs Persist
    // ---------------------------------------------------------

    println("\n======================================")
    println("Cache vs Persist")
    println("======================================")

    println("cache() uses the default storage level.")

    println("persist() allows us to specify the storage level.")

    println("Example:")
    println("persist(StorageLevel.MEMORY_ONLY)")

    // ---------------------------------------------------------
    // 10. When caching can hurt performance
    // ---------------------------------------------------------

    println("\n======================================")
    println("When Caching Can Hurt Performance")
    println("======================================")

    println("1. Dataset is used only once.")
    println("2. Dataset is very large.")
    println("3. Available memory is limited.")
    println("4. Too many datasets are cached.")
    println("5. Cache causes memory pressure.")

    // ---------------------------------------------------------
    // 11. Remove persisted data
    // ---------------------------------------------------------

    persistedTransactions.unpersist()

    println("\n======================================")
    println("Day 12 Completed Successfully")
    println("======================================")

    // Stop Spark
    sc.stop()
  }
}
