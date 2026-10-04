import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object WindowFunctionsExample {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day18-Window-Functions")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val employees = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/employees.csv")

    println("======================================")
    println("Day 18 - Spark Window Functions")
    println("======================================")

    println("\nEmployee Data:")
    employees.show()

    // --------------------------------------------------
    // 1. ROW_NUMBER
    // --------------------------------------------------

    val departmentWindow =
      Window.partitionBy("department")
        .orderBy(col("salary").desc)

    val rowNumberDF = employees
      .withColumn("row_number", row_number().over(departmentWindow))

    println("\n--------------------------------------")
    println("1. ROW_NUMBER - Ranking within Department")
    println("--------------------------------------")

    rowNumberDF.show()

    // --------------------------------------------------
    // 2. RANK
    // --------------------------------------------------

    val rankDF = employees
      .withColumn("rank", rank().over(departmentWindow))

    println("\n--------------------------------------")
    println("2. RANK - Employee Salary Ranking")
    println("--------------------------------------")

    rankDF.show()

    // --------------------------------------------------
    // 3. DENSE_RANK
    // --------------------------------------------------

    val denseRankDF = employees
      .withColumn("dense_rank", dense_rank().over(departmentWindow))

    println("\n--------------------------------------")
    println("3. DENSE_RANK")
    println("--------------------------------------")

    denseRankDF.show()

    // --------------------------------------------------
    // 4. LAG
    // --------------------------------------------------

    val salaryWindow =
      Window.partitionBy("department")
        .orderBy("salary")

    val lagDF = employees
      .withColumn("previous_salary", lag("salary", 1).over(salaryWindow))

    println("\n--------------------------------------")
    println("4. LAG - Previous Salary")
    println("--------------------------------------")

    lagDF.show()

    // --------------------------------------------------
    // 5. LEAD
    // --------------------------------------------------

    val leadDF = employees
      .withColumn("next_salary", lead("salary", 1).over(salaryWindow))

    println("\n--------------------------------------")
    println("5. LEAD - Next Salary")
    println("--------------------------------------")

    leadDF.show()

    // --------------------------------------------------
    // 6. Running Total
    // --------------------------------------------------

    val runningWindow =
      Window.partitionBy("department")
        .orderBy("salary")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    val runningTotalDF = employees
      .withColumn(
        "running_salary_total",
        sum("salary").over(runningWindow)
      )

    println("\n--------------------------------------")
    println("6. Running Salary Total")
    println("--------------------------------------")

    runningTotalDF.show()

    // --------------------------------------------------
    // 7. Department Average Salary
    // --------------------------------------------------

    val averageDF = employees
      .withColumn(
        "department_average_salary",
        avg("salary").over(
          Window.partitionBy("department")
        )
      )

    println("\n--------------------------------------")
    println("7. Department Average Salary")
    println("--------------------------------------")

    averageDF.show()

    // --------------------------------------------------
    // Scenario: Find highest-paid employee in each
    // department
    // --------------------------------------------------

    val topEmployees = rowNumberDF
      .filter(col("row_number") === 1)

    println("\n--------------------------------------")
    println("Scenario - Highest Paid Employee")
    println("--------------------------------------")

    topEmployees
      .select(
        "employee_id",
        "name",
        "department",
        "salary"
      )
      .show()

    println("======================================")
    println("Day 18 Completed Successfully")
    println("======================================")

    spark.stop()
  }
}
