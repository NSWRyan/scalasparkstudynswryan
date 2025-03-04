package ryan.widodo.spark

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.holdenkarau.spark.testing.SparkSessionProvider.sparkSession
import org.apache.spark.sql.DataFrame
import org.scalatest.funsuite.AnyFunSuite
import ryan.widodo.Utils

import java.io.File
import scala.reflect.io.Directory

class Question4SparkTest extends AnyFunSuite with DataFrameSuiteBase {

  test(
    "question4SparkAnswer should correctly count pairs of passengers who have teleported together more than 3 times"
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
      ("1", "1", "", "", "2021-01-01"),
      ("1", "2", "", "", "2021-01-01"),
      ("1", "3", "", "", "2021-01-01"),
      ("2", "1", "", "", "2021-01-01"),
      ("2", "2", "", "", "2021-01-01"),
      ("2", "3", "", "", "2021-01-01"),
      ("3", "1", "", "", "2021-01-01"),
      ("3", "2", "", "", "2021-01-01"),
      ("3", "3", "", "", "2021-01-01"),
      ("4", "2", "", "", "2021-01-01"),
      ("4", "3", "", "", "2021-01-01"),
      ("5", "2", "", "", "2021-01-01"),
      ("5", "3", "", "", "2021-01-01"),
      ("6", "1", "", "", "2021-01-01"),
      ("6", "2", "", "", "2021-01-01")
    ).toDF("teleportId", "passengerId", "from", "to", "date")

    // Register UDFs used in the function
    spark.udf.register("toInt", (s: String) => Utils.parseInt(s))

    // Save the sample data to CSV (for testing purposes)
    teleportData.write
      .option("header", "true")
      .csv(f"file://$teleportFileNameFullPath")

    Question4Spark.question4SparkAnswer(
      spark,
      teleportFileNameFullPath = teleportFileNameFullPath,
      outputDirFullPath = outputDirFullPath
    )

    // Load the result data
    val result: DataFrame = spark.read
      .option("header", "true")
      .csv(s"file:///$outputDirFullPath/Question4Spark.csv")
      .select(
        "Passenger 1 ID",
        "Passenger 2 ID",
        "Number of Teleports together"
      )

    // Expected data
    val expectedData = Seq(
      ("2", "3", "5"),
      ("1", "2", "4")
    ).toDF("Passenger 1 ID", "Passenger 2 ID", "Number of Teleports together")

    // Assert that the result matches the expected data
    assertDataFrameEquals(result, expectedData)
  }
}
