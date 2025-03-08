package ryan.widodo.spark

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import ryan.widodo.Utils
import ryan.widodo.dao.Teleport
import ryan.widodo.answers.Question3Helper.{countriesChain, longestRunNoZZ}

import java.sql.Date

/** Problem: Given a passenger's travel history, find the longest sequence of
  * countries visited without the ZZ.
  *
  * Solution:
  *   1. First group by passengerId and aggregate from, to, and date info.
  *   1. Sort by date for the aggregated info.
  *   1. Convert From and To into a country chain like in the example.
  *   1. Count each country and reset if ZZ.
  *   1. Get the max length.
  */

object Question3Spark {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println(
        "Required args: <teleportDataCSVFullPath> <outputDirFullPath>"
      )
      System.exit(1)
    }
    val teleportFileNameFullPath: String = args(0)
    val outputDirFullPath: String = args(1)

    val spark: SparkSession = SparkSession.builder
      .appName("RandomCompanyHWQ3")
      .getOrCreate()

    question3SparkAnswer(
      spark = spark,
      teleportFileNameFullPath = teleportFileNameFullPath,
      outputDirFullPath = outputDirFullPath
    )

    spark.stop()
  }

  /** A helper case class for PassengerTeleports.
    *
    * @param from
    *   String country from
    * @param to
    *   String country to
    * @param date
    *   Teleport Date
    */
  case class TeleportLite(from: String, to: String, date: Date)

  /** A helper class for PassengerTeleports.
    *
    * @param passengerId
    *   The passengerId in Int
    * @param Teleports
    *   TeleportLite object containing country from and to and the Teleport
    *   date.
    */
  case class PassengerTeleports(
      passengerId: Int,
      Teleports: Seq[TeleportLite]
  )

  /** A helper class for the final DataSet.
    *
    * @param passengerId
    *   Int, the passengerId.
    * @param longestRun
    *   Int, the longest subsequence without ZZ.
    */
  case class LongestRun(passengerId: Int, longestRun: Int)

  /** Spark group by month then count distinct teleportId.
    *
    * @param spark
    *   The SparkSession
    * @param teleportFileNameFullPath
    *   The full path for the teleportData.csv
    * @param outputDirFullPath
    *   The output dir in full path
    */
  def question3SparkAnswer(
      spark: SparkSession,
      teleportFileNameFullPath: String,
      outputDirFullPath: String
  ): Unit = {
    // Required for case class encoding for Dataset.
    import spark.implicits._

    val toInt = udf[Int, String](Utils.parseInt)
    val toDate = udf[Date, String](Utils.parseDate)

    // Load the data
    // Parse the dataframe into dataset
    // Then convert to DataSet.
    // Repartition the dataset such that the groupBy is more efficient.
    val teleportDS: Dataset[Teleport] =
      spark.read
        .option("header", "true")
        .format("csv")
        .load(f"file://${teleportFileNameFullPath}")
        .withColumn("passengerId", toInt(col("passengerId")))
        .withColumn("teleportId", toInt(col("teleportId")))
        .withColumn("date", toDate(col("date")))
        .as[Teleport]
        .repartition(col("passengerId"))

    // We take only the necessary columns, parse it.
    // Then convert to DataSet.
    val teleportGroupedDS: Dataset[PassengerTeleports] =
      teleportDS
        .groupBy("passengerId")
        .agg(collect_list(struct("from", "to", "date")).as("Teleports"))
        .as[PassengerTeleports]

    // Convert the scala function to udf.
    val longestCountriesChainUDF = udf((Teleports: Seq[Row]) => {
      val teleportList = Teleports
        .map(row =>
          Teleport(
            0,
            0,
            row.getString(0),
            row.getString(1),
            row.getDate(2)
          )
        )
        .toList
      longestRunNoZZ(countriesChain(teleportList))
    })

    // Calculate the longest run from the list of Teleports.
    val longestRunDS: Dataset[LongestRun] = teleportGroupedDS
      .withColumn(
        "longestRun",
        longestCountriesChainUDF(col("Teleports"))
      )
      .drop("Teleports")
      .as[LongestRun]

    // Merge the partition into 1 and Write the dataframe
    longestRunDS
      .coalesce(1)
      .sortWithinPartitions(desc("longestRun"), asc("passengerId"))
      .withColumnRenamed("passengerId", "Passenger ID")
      .withColumnRenamed("longestRun", "Longest Run")
      .write
      .option("header", "true")
      .csv(f"file:///${outputDirFullPath}/Question3Spark.csv")
  }
}
