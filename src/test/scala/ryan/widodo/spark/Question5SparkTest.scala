package ryan.widodo.spark

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.holdenkarau.spark.testing.SparkSessionProvider.sparkSession
import org.scalatest.funsuite.AnyFunSuite
import ryan.widodo.Utils
import org.apache.spark.sql.functions._

import java.io.File
import scala.reflect.io.Directory

class Question5SparkTest extends AnyFunSuite with DataFrameSuiteBase {

  test(
    "teleportedTogetherSpark should correctly count passenger pairs who have teleported together at least N times within a date range"
  ) {
    val spark = sparkSession

    import spark.implicits._

    // Mock input file paths
    val sparkTestDirBase = "/tmp/spark_test"
    val teleportFileNameFullPath =
      f"${sparkTestDirBase}/tmp/test-Teleport-data.csv"
    val outputDirFullPath = f"${sparkTestDirBase}/tmp/test-output"
    val outputDirectory = new Directory(new File(sparkTestDirBase))
    outputDirectory.deleteRecursively()

    // Sample Teleport data
    val teleportData = Seq(
      ("1", "1", "2024-01-01"),
      ("1", "2", "2024-01-01"),
      ("1", "3", "2024-01-01"),
      ("2", "1", "2024-01-02"),
      ("2", "2", "2024-01-02"),
      ("2", "3", "2024-01-02"),
      ("3", "1", "2024-01-03"),
      ("3", "2", "2024-01-03"),
      ("3", "3", "2024-01-03"),
      ("4", "2", "2024-01-04"),
      ("4", "3", "2024-01-04"),
      ("5", "2", "2024-01-05"),
      ("5", "3", "2024-01-05"),
      ("6", "1", "2024-01-06"),
      ("6", "2", "2024-01-06"),
      ("7", "4", "2024-01-07"),
      ("7", "5", "2024-01-07")
    ).toDF("teleportId", "passengerId", "date")

    // Register UDFs used in the function
    spark.udf.register("toInt", (s: String) => Utils.parseInt(s))
    spark.udf.register("toDate", (s: String) => Utils.parseDate(s))

    // Call the function with test data
    val atLeastNTimes = 3
    val from = Utils.parseDate("2024-01-01")
    val to = Utils.parseDate("2024-01-05")

    // Save the sample data to CSV (for testing purposes)
    teleportData.write
      .option("header", "true")
      .csv(f"file://$teleportFileNameFullPath")

    Question5Spark.teleportedTogetherSpark(
      spark,
      teleportFileNameFullPath = teleportFileNameFullPath,
      outputDirFullPath = outputDirFullPath,
      from = from,
      to = to,
      atLeastNTimes = atLeastNTimes
    )

    // Load the result data
    val result = spark.read
      .option("header", "true")
      .csv(s"file:///$outputDirFullPath/Question5Spark.csv")
      .select(
        "Passenger 1 ID",
        "Passenger 2 ID",
        "Number of Teleports together",
        "From",
        "To"
      )

    // Expected data
    val expectedData = Seq(
      ("1", "2", "3", "2024-01-01", "2024-01-03"),
      ("1", "3", "3", "2024-01-01", "2024-01-03"),
      ("2", "3", "5", "2024-01-01", "2024-01-05")
    ).toDF(
      "Passenger 1 ID",
      "Passenger 2 ID",
      "Number of Teleports together",
      "From",
      "To"
    ).orderBy(
      desc("Number of Teleports together"),
      asc("Passenger 1 ID"),
      asc("Passenger 2 ID")
    )

    // Assert that the result matches the expected data
    assertDataFrameEquals(expectedData, result)
  }
}
