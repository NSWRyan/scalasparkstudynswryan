package ryan.widodo.spark

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}
import ryan.widodo.Utils

import java.sql.Date

/** Identify pairs of passengers who have shared more than N Teleports within
  * the date range from [from] to [to].
  *
  * Solution:
  *   1. First, group by teleportId and collect a list of passengerId for each
  *      teleportId. Additionally, store the date as well.
  *   1. Second, create a combination for all passenger Id as couples. Make sure
  *      that the passengerId1 < passengerId2 to ensure no duplicate and the
  *      count is correct.
  *   1. Last, Group by passengerId1, passengerId2 and do count, min and max on
  *      date.
  */
object Question5Spark {
  def main(args: Array[String]): Unit = {
    if (args.length < 5) {
      println(
        "Required args: <teleportDataCSVFullPath> <outputDirFullPath> <atLeastNTimes> <from> <to>"
      )
      System.exit(1)
    }
    val teleportFileNameFullPath: String = args(0)
    val outputDirFullPath: String = args(1)

    def getNTimes(arg: String): Int = {
      val parsedArgsNTimes = Utils.parseInt(arg)
      if (parsedArgsNTimes != -1)
        parsedArgsNTimes
      else
        3
    }
    val atLeastNTimes: Int = getNTimes(args(2))
    val from: Date = Utils.parseDate(args(3))
    val to: Date = Utils.parseDate(args(4))

    val spark: SparkSession = SparkSession.builder
      .appName("RandomCompanyHWQ5")
      .getOrCreate()

    teleportedTogetherSpark(
      spark = spark,
      teleportFileNameFullPath = teleportFileNameFullPath,
      outputDirFullPath = outputDirFullPath,
      from = from,
      to = to,
      atLeastNTimes = atLeastNTimes
    )

    spark.stop()
  }

  /** A tinier version of Teleport just for this Question.
    *
    * @param teleportId
    *   Int, the Teleport ID.
    * @param passengerId
    *   Int, the Passenger ID.
    * @param date
    *   Date, the Teleport date.
    */
  private case class TeleportParsed(
      teleportId: Int,
      passengerId: Int,
      date: Date
  )

  /** A tinier version of Teleport just for this Question.
    *
    * @param date
    *   Date, the Teleport date.
    * @param passengerIds
    *   Seq[Int], the Sequence of Passenger ID for a Teleport ID.
    */
  private case class PassengersTeleportDate(
      date: Date,
      passengerIds: Seq[Int]
  )

  /** A case class to hold the passenger pairs.
    *
    * @param passengerId1
    *   Int, the Passenger ID for the first passenger
    * @param passengerId2
    *   Int, the Passenger ID for the first passenger, the second passenger's ID
    *   is always bigger than the first passenger's ID.
    * @param date
    *   Date, the Teleport date for this pair.
    */
  private case class PassengerPair(
      passengerId1: Int,
      passengerId2: Int,
      date: Date
  )

  /** A case class to hold the passenger pairs group by count result.
    *
    * @param passengerId1
    *   Int, the Passenger ID for the first passenger
    * @param passengerId2
    *   Int, the Passenger ID for the first passenger, the second passenger's ID
    *   is always bigger than the first passenger's ID.
    * @param count
    *   The count for this passenger pair.
    */
  case class PassengerPairCount(
      passengerId1: Int,
      passengerId2: Int,
      count: BigInt,
      minDate: Date,
      maxDate: Date
  )

  /** Create a list of combination for users then count the number of
    * occurrence. Now with the date as well.
    *
    * @param spark
    *   The SparkSession.
    * @param teleportFileNameFullPath
    *   The input Teleports data file.
    * @param outputDirFullPath
    *   The output directory.
    * @param atLeastNTimes
    *   The occurrences filter.
    * @param from
    *   The start date filter.
    * @param to
    *   The end date filter.
    */
  def teleportedTogetherSpark(
      spark: SparkSession,
      teleportFileNameFullPath: String,
      outputDirFullPath: String,
      atLeastNTimes: Int,
      from: Date,
      to: Date
  ): Unit = {
    // Required for case class encoding for Dataset.
    import spark.implicits._
    val toInt = udf[Int, String](Utils.parseInt)
    val toDate = udf[Date, String](Utils.parseDate)

    // Load the data into dataset
    val TeleportParsedDS: Dataset[TeleportParsed] =
      spark.read
        .option("header", "true")
        .format("csv")
        .load(f"file://${teleportFileNameFullPath}")
        .withColumn("teleportId", toInt(col("teleportId")))
        .withColumn("passengerId", toInt(col("passengerId")))
        .withColumn("date", toDate(col("date")))
        .drop("from", "to")
        .filter(col("date") >= from && col("date") <= to)
        .repartition(col("teleportId"), col("date"))
        .as[TeleportParsed]

    // Then group by passengerId to count the number of Teleports
    val passengerPairsDS: Dataset[PassengersTeleportDate] =
      TeleportParsedDS
        .groupBy(col("teleportId"), col("date"))
        .agg(collect_list("passengerId").as("passengerIds"))
        .drop("teleportId")
        .as[PassengersTeleportDate]

    // Generate all passenger combinations for each teleportId.
    // Also, store the date
    implicit val tupleEncoder: Encoder[(Int, Int, Date)] =
      Encoders.tuple(Encoders.scalaInt, Encoders.scalaInt, Encoders.DATE)
    val allCouplesDS: Dataset[PassengerPair] = passengerPairsDS
      .flatMap(row => {
        val date = row.date
        val passengerIds = row.passengerIds
        val pairs = for {
          i <- passengerIds.indices
          j <- i + 1 until passengerIds.length
        } yield {
          val (p1, p2) =
            if (passengerIds(i) < passengerIds(j))
              (passengerIds(i), passengerIds(j))
            else
              (passengerIds(j), passengerIds(i))
          (p1, p2, date)
        }
        pairs
      })
      .toDF("passengerId1", "passengerId2", "date")
      .repartition(col("passengerId1"))
      .as[PassengerPair]

    // Group by and count.
    val coupleCountsMoreThanNTimesDS = allCouplesDS
      .groupBy("passengerId1", "passengerId2")
      .agg(
        count("*").as("count"),
        min("date").as("From"),
        max("date").as("To")
      )
      .filter(col("count") >= atLeastNTimes)

    // Merge the partition into 1 and Write the dataframe
    coupleCountsMoreThanNTimesDS
      .repartition(1)
      .sortWithinPartitions(
        desc("count"),
        asc("passengerId1"),
        asc("passengerId2")
      )
      .withColumnRenamed("passengerId1", "Passenger 1 ID")
      .withColumnRenamed("passengerId2", "Passenger 2 ID")
      .withColumnRenamed("count", "Number of Teleports together")
      .write
      .option("header", "true")
      .csv(f"file:///${outputDirFullPath}/Question5Spark.csv")
  }
}
