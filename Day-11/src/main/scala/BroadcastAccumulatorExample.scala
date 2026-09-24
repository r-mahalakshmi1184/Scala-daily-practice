import org.apache.spark.{SparkConf, SparkContext}

object BroadcastAccumulatorExample {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day 11 Broadcast and Accumulator")
      .setMaster("local[2]")

    val sc = new SparkContext(conf)

    println("=== Day 11: Broadcast and Accumulators ===")

    // ---------------------------------------------------------
    // 1. Small product master/reference table
    // ---------------------------------------------------------

    val productReference = Map(
      "P101" -> "Laptop",
      "P102" -> "Phone",
      "P103" -> "Tablet",
      "P104" -> "Monitor"
    )

    // ---------------------------------------------------------
    // 2. Broadcast the small reference map
    // ---------------------------------------------------------

    val broadcastProducts = sc.broadcast(productReference)

    println("\nBroadcast Product Reference:")
    broadcastProducts.value.foreach(println)

    // ---------------------------------------------------------
    // 3. Create accumulator for bad records
    // ---------------------------------------------------------

    val badRecordAccumulator = sc.longAccumulator(
      "Bad Transaction Records"
    )

    // ---------------------------------------------------------
    // 4. Read transaction data
    // ---------------------------------------------------------

    val transactions = sc.textFile("data/transactions.txt")

    // ---------------------------------------------------------
    // 5. Validate transactions using broadcast data
    // ---------------------------------------------------------

    val validTransactions = transactions.filter { line =>

      val parts = line.split(",")

      val transactionId = parts(0)
      val productId = parts(1)
      val accountId = parts(2)
      val amount = parts(3).toDouble

      if (broadcastProducts.value.contains(productId)) {

        true

      } else {

        badRecordAccumulator.add(1)

        println(
          s"Invalid transaction: $transactionId | " +
          s"Product: $productId | " +
          s"Account: $accountId | " +
          s"Amount: $amount"
        )

        false
      }
    }

    // ---------------------------------------------------------
    // 6. Trigger Spark execution
    // ---------------------------------------------------------

    val validTransactionList = validTransactions.collect()

    // ---------------------------------------------------------
    // 7. Display valid transactions
    // ---------------------------------------------------------

    println("\nValid Transactions:")

    validTransactionList.foreach(println)

    // ---------------------------------------------------------
    // 8. Display bad record count
    // ---------------------------------------------------------

    println("\nBad Record Count:")
    println(badRecordAccumulator.value)

    // ---------------------------------------------------------
    // 9. Display total valid transaction count
    // ---------------------------------------------------------

    println("\nValid Transaction Count:")
    println(validTransactionList.length)

    // ---------------------------------------------------------
    // 10. Explanation
    // ---------------------------------------------------------

    println("\nConcept Summary:")

    println(
      "Broadcast: Sends a small read-only dataset to executors."
    )

    println(
      "Accumulator: Allows executors to add values to a shared counter."
    )

    println(
      "Normal driver variables should not be used for distributed updates."
    )

    println(
      "Broadcast is useful when executors repeatedly need a small reference dataset."
    )

    sc.stop()
  }
}
