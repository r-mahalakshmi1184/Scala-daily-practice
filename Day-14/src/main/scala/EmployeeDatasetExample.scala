import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

case class Employee(
  employee_id: String,
  name: String,
  department: String,
  salary: Double
)

object EmployeeDatasetExample {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day 14 DataFrame and Dataset")
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._

    println("======================================")
    println("Day 14 - DataFrame and Dataset")
    println("======================================")

    // ---------------------------------------------------------
    // 1. Create DataFrame from CSV
    // ---------------------------------------------------------

    val employeeDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/employees.csv")

    println("\nEmployee DataFrame:")
    employeeDF.show()

    // ---------------------------------------------------------
    // 2. Display DataFrame schema
    // ---------------------------------------------------------

    println("\nDataFrame Schema:")
    employeeDF.printSchema()

    // ---------------------------------------------------------
    // 3. Convert DataFrame to Dataset
    // ---------------------------------------------------------

    val employeeDS: Dataset[Employee] =
      employeeDF.as[Employee]

    println("\nEmployee Dataset:")
    employeeDS.show()

    // ---------------------------------------------------------
    // 4. Dataset type-safe operation
    // ---------------------------------------------------------

    println("\nEmployees with salary above 60000:")

    employeeDS
      .filter(employee => employee.salary > 60000)
      .show()

    // ---------------------------------------------------------
    // 5. Dataset transformation
    // ---------------------------------------------------------

    val increasedSalaryDS = employeeDS.map { employee =>
      Employee(
        employee.employee_id,
        employee.name,
        employee.department,
        employee.salary * 1.10
      )
    }

    println("\nEmployees after 10% salary increase:")

    increasedSalaryDS.show()

    // ---------------------------------------------------------
    // 6. Convert Dataset back to DataFrame
    // ---------------------------------------------------------

    val increasedSalaryDF =
      increasedSalaryDS.toDF()

    println("\nDataset converted back to DataFrame:")

    increasedSalaryDF.show()

    // ---------------------------------------------------------
    // 7. DataFrame aggregation
    // ---------------------------------------------------------

    println("\nDepartment-wise Average Salary:")

    increasedSalaryDF
      .groupBy("department")
      .agg(
        round(avg("salary"), 2).alias("average_salary"),
        max("salary").alias("maximum_salary"),
        min("salary").alias("minimum_salary")
      )
      .show()

    // ---------------------------------------------------------
    // 8. Catalyst optimization
    // ---------------------------------------------------------

    println("\nExecution Plan:")

    increasedSalaryDF
      .filter(col("salary") > 60000)
      .select("name", "department", "salary")
      .explain(true)

    // ---------------------------------------------------------
    // 9. Comparison
    // ---------------------------------------------------------

    println("\n======================================")
    println("RDD vs DataFrame vs Dataset")
    println("======================================")

    println("RDD:")
    println("- Low-level distributed collection")
    println("- No named column structure")
    println("- More manual optimization")

    println("\nDataFrame:")
    println("- Distributed table with named columns")
    println("- Optimized by Spark SQL and Catalyst")
    println("- No compile-time type safety for column names")

    println("\nDataset:")
    println("- Combines typed objects with Spark optimization")
    println("- Provides compile-time type safety")
    println("- Uses Catalyst optimization")

    println("\n======================================")
    println("Day 14 Completed Successfully")
    println("======================================")

    spark.stop()
  }
}
