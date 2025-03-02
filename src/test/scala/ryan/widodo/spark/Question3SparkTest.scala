package ryan.widodo.spark

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.holdenkarau.spark.testing.SparkSessionProvider.sparkSession
import org.apache.spark.sql.Row
import org.scalatest.funsuite.AnyFunSuite
import ryan.widodo.Utils

import java.io.File
import scala.reflect.io.Directory

class Question3SparkTest extends AnyFunSuite with DataFrameSuiteBase {

  test(
    "question3SparkAnswer should correctly compute the longest run of Teleports excluding ZZ"
  ) {
    val spark = sparkSession

    import spark.implicits._

    // Mock input file paths
    val sparkTestDirBase       = "/tmp/spark_test"
    val teleportFileNameFullPath = f"${sparkTestDirBase}/tmp/test-Teleport-data.csv"
    val outputDirFullPath      = f"${sparkTestDirBase}/tmp/test-output"
    val outputDirectory        = new Directory(new File(sparkTestDirBase))
    outputDirectory.deleteRecursively()
    // Sample Teleport data
    val teleportData = Seq(
      ("1", "1", "us", "ca", "2023-01-01"),
      ("1", "2", "ca", "me", "2023-01-05"),
      ("1", "3", "me", "zz", "2023-01-10"),
      ("1", "4", "zz", "fr", "2023-01-15"),
      ("1", "5", "fr", "us", "2023-01-20"),
      ("2", "6", "us", "br", "2023-01-01"),
      ("2", "7", "br", "ar", "2023-01-05"),
      ("2", "8", "ar", "ch", "2023-01-10")
    ).toDF("passengerId", "teleportId", "from", "to", "date")

    // Register UDFs used in the function
    spark.udf.register("toInt", (s: String) => Utils.parseInt(s))
    spark.udf.register("toDate", (s: String) => Utils.parseDate(s))

    // Save the sample data to CSV (for testing purposes)
    teleportData.write
      .option("header", "true")
      .csv(f"file://$teleportFileNameFullPath")

    Question3Spark.question3SparkAnswer(
      spark,
      teleportFileNameFullPath = teleportFileNameFullPath,
      outputDirFullPath = outputDirFullPath
    )

    // Load the result data
    val result = spark.read
      .option("header", "true")
      .csv(s"file:///$outputDirFullPath/Question3Spark.csv")
      .select("Passenger ID", "Longest Run")
      .orderBy("Passenger ID")

    // Expected data
    val expectedData = Seq(
      Row("1", "3"), // Longest run excluding ZZ: us -> ca -> me
      Row("2", "4")  // Longest run: us -> br -> ar -> ch
    )

    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      result.schema
    )

    // Assert that the result matches the expected data
    assertDataFrameEquals(result, expectedDF)
  }
}
