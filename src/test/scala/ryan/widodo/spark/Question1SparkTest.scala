package ryan.widodo.spark

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.holdenkarau.spark.testing.SparkSessionProvider.sparkSession
import org.apache.spark.sql.Row
import org.scalatest.funsuite.AnyFunSuite
import ryan.widodo.Utils

import java.io.File
import java.sql.Date
import scala.reflect.io.Directory

class Question1SparkTest extends AnyFunSuite with DataFrameSuiteBase {

  test(
    "question1SparkAnswer should correctly group by month and count distinct teleportId"
  ) {
    val spark = sparkSession

    import spark.implicits._

    // Mock input file paths
    val sparkTestDirBase  = "/tmp/spark_test"
    val outputDirFullPath = f"${sparkTestDirBase}/tmp/test-output"
    val outputDirectory   = new Directory(new File(outputDirFullPath))
    outputDirectory.deleteRecursively()
    val inputFilePath = f"${sparkTestDirBase}/tmp/test-Teleport-data.csv"

    // Sample data
    val teleportData = Seq(
      ("1", "1", "a", "b", "2023-01-15"),
      ("2", "2", "a", "b", "2023-01-20"),
      ("3", "3", "a", "b", "2023-02-10"),
      ("4", "4", "a", "b", "2023-02-11"),
      ("5", "2", "a", "b", "2023-02-12"),
      ("6", "3", "a", "b", "2023-03-01")
    ).toDF("passengerId", "teleportId", "from", "to", "date")
      .write
      .option("header", "true")
      .mode("overwrite")
      .csv(inputFilePath)

    // Register UDFs used in the function
    spark.udf.register("toInt", (s: String) => Utils.parseInt(s))
    spark.udf.register("toDate", (s: String) => Utils.parseDate(s))
    spark.udf.register("toMonth", (d: Date) => Utils.getMonth(d))

    // Call the function with test data
    Question1Spark.question1SparkAnswer(
      spark,
      teleportFileNameFullPath = inputFilePath,
      outputDirFullPath = outputDirFullPath
    )

    // Load the result data
    val result = spark.read
      .option("header", "true")
      .csv(s"file:///$outputDirFullPath/Question1Spark.csv")
      .select("Month", "Number of Teleports")

    // Expected data
    val expectedData = Seq(
      Row("1", "2"),
      Row("2", "3"),
      Row("3", "1")
    )

    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      result.schema
    )

    // Assert that the result matches the expected data
    assertDataFrameEquals(result, expectedDF)
  }
}
