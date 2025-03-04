package ryan.widodo.spark

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._
import ryan.widodo.Utils

import java.sql.Date

/** Problem: Calculate the monthly Teleport count.
  *
  * Solution: Extract month from date and do group by count on the month.
  */
object Question1Spark {
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
      .appName("RandomCompanyHWQ1")
      .getOrCreate()

    question1SparkAnswer(
      spark = spark,
      teleportFileNameFullPath = teleportFileNameFullPath,
      outputDirFullPath = outputDirFullPath
    )

    spark.stop()
  }

  /** Case Class helper for storing month and count as DataSet.
    *
    * @param month
    *   Int
    * @param count
    *   Int
    */
  private case class Teleportsummary(month: Int, count: Int)

  /** Case Class helper for storing a tinier Teleport data necessary for this
    * calculation..
    *
    * @param teleportId
    *   Int, the Teleport ID.
    * @param month
    *   Int, the month of the Teleport from date.
    */
  private case class TeleportLite(teleportId: Int, month: Int)

  /** Generate the number of Teleports for each month using dataset.
    *
    * @param spark
    *   SparkSession
    * @param teleportFileNameFullPath
    *   The full path to the input Teleport data csv.
    * @param outputDirFullPath
    *   The output dir for the output CSV.
    */
  def question1SparkAnswer(
      spark: SparkSession,
      teleportFileNameFullPath: String,
      outputDirFullPath: String
  ): Unit = {
    // Required for case class encoding for Dataset.
    import spark.implicits._

    val toInt = udf[Int, String](Utils.parseInt)
    val toDate = udf[Date, String](Utils.parseDate)
    val toMonth = udf[Int, Date](Utils.getMonth)

    // Load the data and
    // parse from string to the appropriate types.
    val TeleportLiteDS: Dataset[TeleportLite] =
      spark.read
        .option("header", "true")
        .format("csv")
        .load(f"file://${teleportFileNameFullPath}")
        .withColumn("teleportId", toInt(col("teleportId")))
        .withColumn("month", toMonth(toDate(col("date"))))
        .as[TeleportLite]

    // Group by month and count distinct teleportId
    val groupByCountDistinctteleportIdDS: Dataset[Teleportsummary] =
      TeleportLiteDS
        .groupByKey(_.month)
        .mapGroups { case (month, iter) =>
          val teleportIds = iter.map(_.teleportId).toSet
          Teleportsummary(month, teleportIds.size)
        }

    // Merge the partition into 1 and Write the dataset
    groupByCountDistinctteleportIdDS
      .repartition(1) // Just to make the CSV readable.
      .sortWithinPartitions(asc("month"))
      .withColumnRenamed("month", "Month")
      .withColumnRenamed("count", "Number of Teleports")
      .write
      .option("header", "true")
      .csv(f"file:///${outputDirFullPath}/Question1Spark.csv")
  }
}
