import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object WordCount {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day 6 Word Count")
      .setMaster("local[2]")

    val sc = new SparkContext(conf)

    println("=== Day 6: Classic Word Count ===")

    // Read the input file
    val lines = sc.textFile("data/input.txt")

    // Step 1: flatMap - split lines into individual words
    val words = lines
  .flatMap(line => line.split("\\s+"))
  .map(word => word.replaceAll("[^a-zA-Z0-9]", "").toLowerCase)
  .filter(_.nonEmpty)

    // Step 2: map - convert each word into (word, 1)
    val pairs = words.map(word => (word, 1))

    // Step 3: reduceByKey - add counts for the same word
    val counts = pairs.reduceByKey((a, b) => a + b)

    // Display the result
    println("\nWord Counts:")
    counts.collect().foreach(println)
// Find the top 10 most frequent words
val top10 = counts
  .sortBy { case (_, count) => count }
  .top(10)

println("\nTop 10 Most Frequent Words:")
top10.foreach(println)
    sc.stop()
  }
}
