import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object AggregationsExample {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day 16 Aggregations")
      .master("local[2]")
      .getOrCreate()

    println("======================================")
    println("Day 16 - Aggregations")
    println("======================================")

    // ---------------------------------------------------------
    // 1. Read hospital revenue data
    // ---------------------------------------------------------

    val hospitalDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/hospital_revenue.csv")

    println("\nHospital Revenue Data:")
    hospitalDF.show()

    // ---------------------------------------------------------
    // 2. Basic Aggregations
    // ---------------------------------------------------------

    println("\n======================================")
    println("Basic Aggregations")
    println("======================================")

    val totalPatients = hospitalDF.count()

    val totalRevenue = hospitalDF
      .agg(sum("revenue"))
      .first()
      .get(0)

    val averageRevenue = hospitalDF
      .agg(avg("revenue"))
      .first()
      .get(0)

    val minimumRevenue = hospitalDF
      .agg(min("revenue"))
      .first()
      .get(0)

    val maximumRevenue = hospitalDF
      .agg(max("revenue"))
      .first()
      .get(0)

    println(s"Total Patients: $totalPatients")
    println(s"Total Revenue: $totalRevenue")
    println(s"Average Revenue: $averageRevenue")
    println(s"Minimum Revenue: $minimumRevenue")
    println(s"Maximum Revenue: $maximumRevenue")

    // ---------------------------------------------------------
    // 3. All aggregations together
    // ---------------------------------------------------------

    println("\nAll Aggregations:")

    hospitalDF
      .agg(
        count("*").alias("patient_count"),
        sum("revenue").alias("total_revenue"),
        round(avg("revenue"), 2).alias("average_revenue"),
        min("revenue").alias("minimum_revenue"),
        max("revenue").alias("maximum_revenue")
      )
      .show()

    // ---------------------------------------------------------
    // 4. Department-wise statistics
    // ---------------------------------------------------------

    println("\n======================================")
    println("Department-wise Statistics")
    println("======================================")

    hospitalDF
      .groupBy("department")
      .agg(
        count("*").alias("patient_count"),
        sum("revenue").alias("total_revenue"),
        round(avg("revenue"), 2).alias("average_revenue"),
        min("revenue").alias("minimum_revenue"),
        max("revenue").alias("maximum_revenue")
      )
      .orderBy(desc("total_revenue"))
      .show()

    // ---------------------------------------------------------
    // 5. Group by multiple columns
    // ---------------------------------------------------------

    println("\n======================================")
    println("Department and Doctor-wise Statistics")
    println("======================================")

    hospitalDF
      .groupBy("department", "doctor")
      .agg(
        count("*").alias("patient_count"),
        sum("revenue").alias("total_revenue"),
        round(avg("revenue"), 2).alias("average_revenue")
      )
      .orderBy("department")
      .show()

    // ---------------------------------------------------------
    // 6. HAVING-like filtering
    // ---------------------------------------------------------

    println("\n======================================")
    println("Departments with Revenue > 30000")
    println("======================================")

    hospitalDF
      .groupBy("department")
      .agg(
        sum("revenue").alias("total_revenue")
      )
      .filter(col("total_revenue") > 30000)
      .show()

    // ---------------------------------------------------------
    // 7. Hospital Revenue Report
    // ---------------------------------------------------------

    println("\n======================================")
    println("Hospital Department Revenue Report")
    println("======================================")

    hospitalDF
      .groupBy("department")
      .agg(
        count("*").alias("total_patients"),
        sum("revenue").alias("total_revenue"),
        round(avg("revenue"), 2).alias("average_revenue"),
        min("revenue").alias("minimum_revenue"),
        max("revenue").alias("maximum_revenue")
      )
      .orderBy(desc("total_revenue"))
      .show()

    println("\n======================================")
    println("Day 16 Completed Successfully")
    println("======================================")

    spark.stop()
  }
}
