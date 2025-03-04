package ryan.widodo.spark

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._
import ryan.widodo.Utils
import ryan.widodo.dao.Passenger

import java.sql.Date

/** Problem: Identify the 100 passengers with the highest Teleport frequency.
  *
  * This is the outer join version. If no passenger info found, it will be
  * replaced with unknown.
  */
object Question2Spark {
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      println(
        "Required args: <teleportDataCSVFullPath> <passengersDataCSVFullPath> <outputDirFullPath>"
      )
      System.exit(1)
    }
    val teleportFileNameFullPath: String = args(0)
    val passengersDataCSVFullPath: String = args(1)
    val outputDirFullPath: String = args(2)

    val spark: SparkSession = SparkSession.builder
      .appName("RandomCompanyHWQ2")
      .getOrCreate()

    question2SparkAnswer(
      spark = spark,
      teleportFileNameFullPath = teleportFileNameFullPath,
      passengersDataCSVFullPath = passengersDataCSVFullPath,
      outputDirFullPath = outputDirFullPath
    )

    spark.stop()
  }

  /** Helper case class for Teleport frequency dataset,
    *
    * @param passengerId
    *   Int, the passenger ID.
    * @param numberOfTeleports
    *   BigInt, the number of Teleports for this passengerId.
    */
  private case class TeleportFrequency(
      passengerId: Int,
      numberOfTeleports: BigInt
  )

  /** Helper case class for Teleport frequency dataset,
    *
    * @param passengerId
    *   Int, the passenger ID.
    * @param teleportId
    *   Int, the Teleport ID.
    */
  private case class TeleportLite(passengerId: Int, teleportId: BigInt)

  /** Helper case class for Teleport frequency dataset,
    *
    * @param passengerId
    *   Int, the passenger ID.
    * @param numberOfTeleports
    *   BigInt, the number of Teleports for this passengerId.
    * @param firstName
    *   String, passenger first name.
    * @param lastName
    *   String, passenger last name.
    */
  private case class PassengerTeleportDetail(
      passengerId: Int,
      numberOfTeleports: BigInt,
      firstName: String,
      lastName: String
  )

  /** Spark group by passengerId then count the teleportId. Then we join with
    * the passengers.csv to fill the passenger data.
    * @param spark
    *   The SparkSession
    * @param teleportFileNameFullPath
    *   The full path for the teleportData.csv
    * @param passengersDataCSVFullPath
    *   The full path for the teleportData.csv
    * @param outputDirFullPath
    *   The output dir in full path
    */
  def question2SparkAnswer(
      spark: SparkSession,
      teleportFileNameFullPath: String,
      passengersDataCSVFullPath: String,
      outputDirFullPath: String
  ): Unit = {
    // Required for case class encoding for Dataset.
    import spark.implicits._

    val toInt = udf[Int, String](Utils.parseInt)

    // Load the data
    val TeleportDS: Dataset[TeleportLite] =
      spark.read
        .option("header", "true")
        .format("csv")
        .load(f"file://${teleportFileNameFullPath}")
        .withColumn("passengerId", toInt(col("passengerId")))
        .withColumn("teleportId", toInt(col("teleportId")))
        .as[TeleportLite]

    // Transform the dataframe to dataset
    // Then group by passengerId to count the number of Teleports
    // Also sort to make the sort-merge more efficient at join.
    val top100PassengerTeleportFrequencyDS: Dataset[TeleportFrequency] =
      TeleportDS
        .groupByKey(_.passengerId)
        .mapGroups { case (passengerId, iter) =>
          val teleportIds = iter.map(_.teleportId).toSet
          TeleportFrequency(passengerId, teleportIds.size)
        }
        .sort(desc("numberOfTeleports"))
        .limit(100)
        .as[TeleportFrequency]
        .repartition(col("passengerId"))
        .sortWithinPartitions("passengerId")

    // Retrieve the passenger data
    val passengerDS: Dataset[Passenger] =
      spark.read
        .option("header", "true")
        .format("csv")
        .load(f"file://${passengersDataCSVFullPath}")
        .withColumn("passengerId", toInt(col("passengerId")))
        .as[Passenger]
        .repartition(col("passengerId"))
        .sortWithinPartitions("passengerId")

    // Inner join
    val joinedDS: Dataset[PassengerTeleportDetail] =
      top100PassengerTeleportFrequencyDS
        .join(passengerDS, Seq("passengerId"))
        .as[PassengerTeleportDetail]

    // Left outer join
    //    val joinedDS: Dataset[Row] = top100PassengerTeleportFrequencyDS
    //      .join(passengerDS, Seq("passengerId"), "left-outer")
    //      .sort(desc("numberOfTeleports"), asc("passengerId"))

    // Merge the partition into 1 and Write the dataframe
    joinedDS
      .repartition(1)
      .sortWithinPartitions(desc("numberOfTeleports"), asc("passengerId"))
      .withColumnRenamed("passengerId", "Passenger ID")
      .withColumnRenamed("numberOfTeleports", "Number of Teleports")
      .withColumnRenamed("firstName", "First Name")
      .withColumnRenamed("lastName", "Last Name")
      .write
      .option("header", "true")
      .csv(f"file:///${outputDirFullPath}/Question2Spark.csv")
  }
}
