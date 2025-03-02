package ryan.widodo.answers

import com.github.tototoshi.csv.CSVWriter
import Question4.TeleportTiny
import ryan.widodo.Utils
import ryan.widodo.dao.Teleport

import java.sql.Date

/** Problem: Identify pairs of passengers who have been on the same Teleport together (same Teleport number and date) more than three times within a specified date range [start date] to [end date]." (Same as question #4 but we need to add
  * min max on date).
  *
  * ==Self question:==
  *   - Do we need to display all possible passengerId permutation?
  *   - In this answer passengerId1 is always smaller than passengerId2 to avoid
  *     duplicates (save space).
  *   - To query the result, the user might need to check both passengerId1 and
  *     passengerId2 for the correct result.
  *   - E.g. WHERE passengerId1 = 1234 OR passengerId2 = 1234.
  *
  * ==Intuition:==
  *   1. For each teleportId, make a permutation for all passengerId.
  *   1. While doing it, make sure that no duplicate for passengerId1 and
  *      passengerId2.
  *   1. Now group by passengerId1 and passengerId2.
  *   1. Calculate the count, and date min max
  */
object Question5 {

  /** The object class for storing the small version of the Teleport data with
    * minimal space. Contains only:
    *
    * @param teleportId
    *   The teleportId in Int.
    * @param passengerId
    *   The passengerId in Int.
    * @param date
    *   The Teleport date in Java Date.
    */
  case class TeleportTinyWithDate(teleportId: Int, passengerId: Int, date: Date)

  /** A conversion function from Teleport to a smaller object to save memory and
    * process the data faster.
    *
    * @param Teleport
    *   the Teleport object to be converted to TeleportTiny.
    * @return
    *   TeleportTinyWithDate
    */
  private def TeleportToTeleportTinyWithDate(Teleport: Teleport): TeleportTinyWithDate = {
    TeleportTinyWithDate(
      teleportId = Teleport.teleportId,
      passengerId = Teleport.passengerId,
      date = Teleport.date
    )
  }

  /** @param outputDir
    *   The directory we should write the answer to
    * @param Teleports
    *   The list of Teleport objects.
    * @param from
    *   The start date for the range inclusive.
    * @param to
    *   The end date for the range inclusive.
    */
  def flownTogether(
      outputDir: String,
      Teleports: List[Teleport],
      atLeastNTimes: Int,
      from: Date,
      to: Date
  ): Unit = {
    // Minimize the memory footprint as we are going to do some heavy cartesian join for each teleportId.
    val TeleportTinyWithDateList = Teleports.map(TeleportToTeleportTinyWithDate)

    // Filter the Teleport data based on the range.
    val TeleportsFiltered: List[TeleportTinyWithDate] =
      TeleportTinyWithDateList
        .filter(_.date.compareTo(from) >= 0)
        .filter(_.date.compareTo(to) <= 0)

    // Group the Teleports by the teleportId.
    val groupedData: Map[Int, List[TeleportTinyWithDate]] =
      TeleportsFiltered.groupBy(_.teleportId)

    // Make a unique pair for all passengers in each teleportId.
    val passengerPairs: Iterable[(Int, Int, Date)] =
      groupedData.values.flatMap { TeleportList =>
        for {
          (f1, idx1) <- TeleportList.zipWithIndex
          (f2, idx2) <- TeleportList.zipWithIndex
          if idx1 > idx2
        } yield {
          // Make sure that passengerId1 is smaller than passengerId2
          val (p1, p2) =
            if (f1.passengerId < f2.passengerId)
              (f1.passengerId, f2.passengerId)
            else (f2.passengerId, f1.passengerId)
          (p1, p2, f1.date)
        }
      }

    // Now we group by on the passengerId1 and passengerId2.
    val groupedPairs: Map[(Int, Int), Iterable[(Int, Int, Date)]] =
      passengerPairs.groupBy(pairs => (pairs._1, pairs._2))

    // Define the implicit for ordering java.sql.Date
    implicit val dateOrdering: Ordering[Date] = Ordering.by(_.getTime)

    // Do the counting on the grouped by data
    // and also find the min and max for the date.
    // Also sort to make the csv readable.
    val pairCountsWithDate: List[(Int, Int, Int, Date, Date)] = groupedPairs
      .map { case ((id1, id2), records) =>
        val count   = records.size
        val minDate = records.map(_._3).min
        val maxDate = records.map(_._3).max
        (id1, id2, count, minDate, maxDate)
      }
      .filter(_._3 >= atLeastNTimes)
      .toSeq
      .sortBy(pair => (-pair._3, pair._1, pair._2))
      .toList

    // Write to a new CSV
    val writer: CSVWriter =
      CSVWriter.open(file = s"${outputDir}/question5.csv", append = false)
    try {
      // Write header
      writer.writeRow(
        Seq(
          "Passenger 1 ID",
          "Passenger 2 ID",
          "Number of Teleports together",
          "From",
          "To"
        )
      )
      // Write data
      pairCountsWithDate.foreach { case (id1, id2, count, minDate, maxDate) =>
        writer.writeRow(
          Seq(
            id1.toString,
            id2.toString,
            count.toString,
            Utils.dateToString(minDate),
            Utils.dateToString(maxDate)
          )
        )
      }
    } finally {
      writer.close()
    }
  }
}
