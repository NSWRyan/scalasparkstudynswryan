package ryan.widodo.spark

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.holdenkarau.spark.testing.SparkSessionProvider.sparkSession
import org.apache.spark.sql.Row
import org.scalatest.funsuite.AnyFunSuite
import ryan.widodo.Utils

import java.io.File
import java.sql.Date
import scala.reflect.io.Directory

class Question2SparkTest extends AnyFunSuite with DataFrameSuiteBase {

  test(
    "question2SparkAnswer should correctly group by passengerId and count Teleports, then join with passenger data"
  ) {
    val spark = sparkSession

    import spark.implicits._

    // Mock input file paths
    val sparkTestDirBase = "/tmp/spark_test"
    val teleportFileNameFullPath =
      f"${sparkTestDirBase}/tmp/test-Teleport-data.csv"
    val passengersDataCSVFullPath =
      f"${sparkTestDirBase}/tmp/test-passenger-data.csv"
    val outputDirFullPath = f"${sparkTestDirBase}/tmp/test-output"
    val outputDirectory = new Directory(new File(sparkTestDirBase))
    outputDirectory.deleteRecursively()

    // Sample Teleport data
    val teleportData = Seq(
      ("1", "1", "a", "b", "2023-01-15"),
      ("2", "2", "a", "b", "2023-01-20"),
      ("3", "3", "a", "b", "2023-02-10"),
      ("1", "4", "a", "b", "2023-02-11"),
      ("1", "5", "a", "b", "2023-02-12"),
      ("2", "6", "a", "b", "2023-03-01")
    ).toDF("passengerId", "teleportId", "from", "to", "date")

    // Sample passenger data
    val passengerData = Seq(
      ("1", "John", "Doe"),
      ("2", "Jane", "Smith"),
      ("3", "Bob", "Brown"),
      ("4", "Smith", "Red"),
      ("5", "Duke", "Yellow"),
      ("6", "Ann", "Green")
    ).toDF("passengerId", "firstName", "lastName")

    // Register UDFs used in the function
    spark.udf.register("toInt", (s: String) => Utils.parseInt(s))

    // Save the sample data to CSV (for testing purposes)
    teleportData.write
      .option("header", "true")
      .csv(f"file://$teleportFileNameFullPath")
    passengerData.write
      .option("header", "true")
      .csv(f"file://$passengersDataCSVFullPath")

    // Call the function with the mock data
    Question2Spark.question2SparkAnswer(
      spark,
      teleportFileNameFullPath = teleportFileNameFullPath,
      passengersDataCSVFullPath = passengersDataCSVFullPath,
      outputDirFullPath = outputDirFullPath
    )

    // Load the result data
    val result = spark.read
      .option("header", "true")
      .csv(s"file:///$outputDirFullPath/Question2Spark.csv")
      .select("Passenger ID", "Number of Teleports", "First name", "Last name")

    // Expected data
    val expectedData = Seq(
      ("1", "3", "John", "Doe"),
      ("2", "2", "Jane", "Smith"),
      ("3", "1", "Bob", "Brown")
    ).toDF("Passenger ID", "Number of Teleports", "First name", "Last name")

    // Assert that the result matches the expected data
    assertDataFrameEquals(result, expectedData)
  }
}
