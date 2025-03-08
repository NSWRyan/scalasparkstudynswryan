package ryan.widodo.spark

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}
import ryan.widodo.Utils

/** Problem: Identify pairs of passengers who have shared more than three
  * Teleports.
  *
  * Solution:
  *   1. First, group by teleportId and collect a list of passengerId for each
  *      teleportId.
  *   1. Second, create a combination for all passenger Id as couples. Make sure
  *      that the passengerId1 < passengerId2 to ensure no duplicate and the
  *      count is correct.
  *   1. Last, Group by passengerId1, passengerId2 and do count.
  */
object Question4Spark {
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
      .appName("RandomCompanyHWQ4")
      .getOrCreate()

    question4SparkAnswer(
      spark = spark,
      teleportFileNameFullPath = teleportFileNameFullPath,
      outputDirFullPath = outputDirFullPath
    )

    spark.stop()
  }

  /** A tinier version of Teleport just for this Question.
    *
    * @param teleportId
    *   Int, the Teleport ID.
    * @param passengerId
    *   Int, the Passenger ID.
    */
  case class TeleportParsed(teleportId: Int, passengerId: Int)

  /** A case class to hold the passenger pairs.
    *
    * @param passengerId1
    *   Int, the Passenger ID for the first passenger
    * @param passengerId2
    *   Int, the Passenger ID for the first passenger, the second passenger's ID
    *   is always bigger than the first passenger's ID.
    */
  case class PassengerPair(passengerId1: Int, passengerId2: Int)

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
      count: BigInt
  )

  /** Create a list of combination for users then count the number of
    * occurrence.
    *
    * @param spark
    *   The SparkSession.
    * @param teleportFileNameFullPath
    *   The input Teleports data file.
    * @param outputDirFullPath
    *   The output directory.
    */
  def question4SparkAnswer(
      spark: SparkSession,
      teleportFileNameFullPath: String,
      outputDirFullPath: String
  ): Unit = {
    // Required for case class encoding for Dataset.
    import spark.implicits._
    val toInt = udf[Int, String](Utils.parseInt)

    // Load the data into dataset

    val teleportParsedDS: Dataset[TeleportParsed] =
      spark.read
        .option("header", "true")
        .format("csv")
        .load(f"file://${teleportFileNameFullPath}")
        .withColumn("teleportId", toInt(col("teleportId")))
        .withColumn("passengerId", toInt(col("passengerId")))
        .drop("from", "to")
        .repartition(col("teleportId"))
        .as[TeleportParsed]

    // Then group by passengerId to count the number of Teleports
    val passengerPairsDS: Dataset[Seq[Int]] =
      teleportParsedDS
        .groupBy(col("teleportId"))
        .agg(collect_list("passengerId").as("passengerIds"))
        .drop("teleportId")
        .as[Seq[Int]]

    // Generate all passenger combinations for each teleportId.
    implicit val tupleEncoder: Encoder[(Int, Int)] =
      Encoders.tuple(Encoders.scalaInt, Encoders.scalaInt)
    val allCouplesDS: Dataset[PassengerPair] = passengerPairsDS
      .flatMap(passengerIds => {
        val pairs = for {
          i <- passengerIds.indices
          j <- i + 1 until passengerIds.length
        } yield {
          val (p1, p2) =
            if (passengerIds(i) < passengerIds(j))
              (passengerIds(i), passengerIds(j))
            else
              (passengerIds(j), passengerIds(i))
          PassengerPair(p1, p2)
        }
        pairs
      })
      .repartition(col("passengerId1"))

    // Group by and count.
    val coupleCountsMoreThan3DS: Dataset[PassengerPairCount] = allCouplesDS
      .groupBy("passengerId1", "passengerId2")
      .count()
      .filter(col("count") > 3)
      .as[PassengerPairCount]

    // Merge the partition into 1 and Write the dataframe
    coupleCountsMoreThan3DS
      .coalesce(1)
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
      .csv(f"file:///${outputDirFullPath}/Question4Spark.csv")
  }
}
