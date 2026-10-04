import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object RealTimeBookingProject {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
  .appName("Day28-Real-Time-Booking")
  .master("local[*]")
  .config("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
  .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    println("Serializer: " + spark.conf.get("spark.serializer", "NOT_SET"))

    println("==========================================")
    println("Day 28 - Real-Time Booking Project")
    println("==========================================")

    // --------------------------------------------------
    // 1. Read booking events
    // --------------------------------------------------

    val bookings = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/bookings.csv")

    println("\n--- Booking Events ---")
    bookings.show(false)

    // --------------------------------------------------
    // 2. Pair RDD - calculate booking count by type
    // --------------------------------------------------

    val bookingPairRDD = bookings.rdd
  .map(row => {
    val routeType = row.getAs[String]("route_type")
    val eventType = row.getAs[String]("event_type")
    (routeType + "_" + eventType, 1)
  })
  .partitionBy(new org.apache.spark.HashPartitioner(4))
  .reduceByKey(_ + _)

    // --------------------------------------------------
    // 3. Broadcast reference data
    // --------------------------------------------------

    val capacityReference = Map(
      "FLIGHT" -> 100,
      "HOTEL" -> 50,
      "BUS" -> 40
    )

    val broadcastCapacity =
      spark.sparkContext.broadcast(capacityReference)

    // --------------------------------------------------
    // 4. Calculate active bookings
    // --------------------------------------------------

    val activeBookings = bookings
      .groupBy("booking_id")
      .agg(
        last("event_type").alias("latest_event"),
        last("route_type").alias("route_type"),
        last("location").alias("location"),
        last("amount").alias("amount")
      )
      .filter(col("latest_event") === "BOOKING")

    println("\n--- Active Bookings ---")
    activeBookings.show(false)

    // --------------------------------------------------
    // 5. Occupancy calculation
    // --------------------------------------------------

    val occupancy = activeBookings
      .groupBy("route_type")
      .agg(
        count("*").alias("active_bookings")
      )
      .collect()
      .map(row => {
        val routeType = row.getAs[String]("route_type")
        val active = row.getAs[Long]("active_bookings")
        val capacity = broadcastCapacity.value.getOrElse(routeType, 0)

        (
          routeType,
          active,
          capacity,
          capacity - active
        )
      })

    println("\n--- Occupancy and Availability ---")
    println("Route Type | Active Bookings | Capacity | Available")

    occupancy.foreach {
      case (routeType, active, capacity, available) =>
        println(
          s"$routeType | $active | $capacity | $available"
        )
    }

    // --------------------------------------------------
    // 6. Spark SQL reporting
    // --------------------------------------------------

    activeBookings.createOrReplaceTempView("active_bookings")

    val report = spark.sql(
      """
        SELECT
          route_type,
          location,
          COUNT(*) AS booking_count,
          SUM(amount) AS total_revenue,
          AVG(amount) AS average_booking_amount
        FROM active_bookings
        GROUP BY route_type, location
        ORDER BY total_revenue DESC
      """
    )

    println("\n--- Spark SQL Booking Report ---")
    report.show(false)

    // --------------------------------------------------
    // 7. Window operation
    // --------------------------------------------------

    val rankingWindow =
      Window.partitionBy("route_type")
        .orderBy(col("amount").desc)

    val rankedBookings = activeBookings
      .withColumn(
        "booking_rank",
        rank().over(rankingWindow)
      )

    println("\n--- Window Ranking ---")
    rankedBookings.show(false)

    println("\n==========================================")
    println("Day 28 processing completed successfully")
    println("==========================================")

    spark.stop()
  }
}
