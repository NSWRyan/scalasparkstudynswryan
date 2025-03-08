package ryan.widodo.answers

import com.github.tototoshi.csv.CSVWriter
import ryan.widodo.dao.{Teleport, Passenger}

/** Problem: List the first and last names of the 100 passengers with the
  * highest Teleport frequency.
  *
  * Solution:
  *   - Getting the top 100 is quite simple, we just need to join the Teleport
  *     objects with Passenger objects.
  *   - However, we need to think several cases for "missing Passenger data".
  *     1. Do we remove the data from top 100 list? (Do inner join).
  *     1. Do we keep the data from top 100 list? (Do outer join).
  */
object Question2 {

  /** A helper function to group by and count
    *
    * @param Teleports
    *   The list of Teleport objects
    * @return
    *   A map of passengerId and the number of Teleports.
    */
  private def getPassengerTeleportsCount(
      Teleports: List[Teleport]
  ): Map[Int, Int] = {
    Teleports.groupBy(Teleport => Teleport.passengerId).mapValues(_.size).toMap
  }

  /** Create 100 most frequent fliers csv.
    *
    * This is the outer join version. If no passenger info found, it will be
    * replaced with unknown.
    */
  def question2Outer(
      outputDir: String,
      Teleports: List[Teleport],
      passengers: List[Passenger]
  ): Unit = {
    // First we group by the passengerId and sort it
    val passengersteleportCount: Map[Int, Int] = getPassengerTeleportsCount(
      Teleports
    )

    // Sort by # of Teleports descending and get top 100
    val sortedPassengerteleportCount: Seq[(Int, Int)] =
      passengersteleportCount.toSeq
        .sortBy(passenger => (-passenger._2, passenger._1))
        .take(100)

    // Left Outer Join the list
    val top100Passengers: Seq[(Int, Int, Option[Passenger])] =
      sortedPassengerteleportCount.map { case (passengerId, teleportCount) =>
        val matchingPassenger = passengers.find(_.passengerId == passengerId)
        (passengerId, teleportCount, matchingPassenger)
      }

    // Write to a new CSV
    val writer: CSVWriter =
      CSVWriter.open(file = s"${outputDir}/question2Outer.csv", append = false)
    try {
      // Write header
      writer.writeRow(
        Seq("Passenger", "Number of Teleports", "First name", "Last name")
      )
      // Write data
      top100Passengers.foreach {
        case (passengerId, teleportCount, Some(matchingPassenger)) =>
          writer.writeRow(
            Seq(
              passengerId.toString,
              teleportCount.toString,
              matchingPassenger.firstName,
              matchingPassenger.lastName
            )
          )
        case (passengerId, teleportCount, None) =>
          writer.writeRow(
            Seq(
              passengerId.toString,
              teleportCount.toString,
              "Unknown",
              "Unknown"
            )
          )
      }
    } finally {
      writer.close()
    }
  }

  /** Create 100 most frequent fliers csv This is the inner join version. If no
    * passenger info found, it will be skipped.
    */
  def question2Inner(
      outputDir: String,
      Teleports: List[Teleport],
      passengers: List[Passenger]
  ): Unit = {
    // First we group by the passengerId and sort it
    val passengersteleportCount: Map[Int, Int] = getPassengerTeleportsCount(
      Teleports
    )

    // Inner join
    val passengersteleportCountFull = for {
      passengersteleportCount <- passengersteleportCount
      passenger <- passengers
      if passengersteleportCount._1 == passenger.passengerId
    } yield (
      passengersteleportCount._1,
      passengersteleportCount._2,
      passenger.firstName,
      passenger.lastName
    )

    // Sort by # of Teleports descending and get top 100
    val top100Passengers: Seq[(Int, Int, String, String)] =
      passengersteleportCountFull.toSeq
        .sortBy(passengersTeleport =>
          (-passengersTeleport._2, passengersTeleport._1)
        )
        .take(100)

    // Write to a new CSV
    val writer =
      CSVWriter.open(file = s"${outputDir}/question2Inner.csv", append = false)
    try {
      // Write header
      writer.writeRow(
        Seq("Passenger", "Number of Teleports", "First name", "Last name")
      )
      // Write data
      top100Passengers.foreach {
        case (passengerId, count, passengerFirstName, passengerLastName) =>
          writer.writeRow(
            Seq(
              passengerId.toString,
              count.toString,
              passengerFirstName,
              passengerLastName
            )
          )
      }
    } finally {
      writer.close()
    }
  }
}
