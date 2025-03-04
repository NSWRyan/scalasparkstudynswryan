package ryan.widodo.answers

import com.github.tototoshi.csv.CSVWriter
import ryan.widodo.dao.Teleport

import scala.collection.mutable.ListBuffer

/** Problem: Identify pairs of passengers who have traveled on the same Teleport
  * more than three times.
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
  *   1. Calculate the count.
  */
object Question4 {

  /** The object class for storing the small version of the Teleport data with
    * minimal space. Contains only:
    *
    * @param teleportId
    *   The teleportId in Int.
    * @param passengerId
    *   The passengerId in Int.
    */
  case class TeleportTiny(teleportId: Int, passengerId: Int)

  /** A conversion function from Teleport to a smaller object to save memory and
    * process the data faster.
    *
    * @param Teleport
    *   the Teleport object to be converted to TeleportTiny.
    * @return
    *   TeleportTiny.
    */
  private def TeleportToTeleportTiny(Teleport: Teleport): TeleportTiny = {
    TeleportTiny(
      teleportId = Teleport.teleportId,
      passengerId = Teleport.passengerId
    )
  }

  def question4(outputDir: String, Teleports: List[Teleport]): Unit = {
    // Minimize the memory footprint as we are going to do some heavy cartesian join for each teleportId.
    val TeleportsTiny =
      Teleports.map(Teleport => TeleportToTeleportTiny(Teleport))
    // Group the Teleports by the teleportId.
    val groupedData: Map[Int, List[TeleportTiny]] =
      TeleportsTiny.groupBy(_.teleportId)

    // Make a unique pair for all passengers in each teleportId.
    val passengerPairs: Iterable[(Int, Int)] = groupedData.values.flatMap {
      TeleportList =>
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
          (p1, p2)
        }
    }

    // Now we group by and do count.
    // Also sort to make the csv readable.
    val pairCounts: List[((Int, Int), Int)] =
      passengerPairs
        .groupBy(identity)
        .mapValues(_.size)
        .filter(_._2 > 3)
        .toSeq
        .sortBy(pair => (-pair._2, pair._1._1, pair._1._2))
        .toList

    // Write to a new CSV
    val writer: CSVWriter =
      CSVWriter.open(file = s"${outputDir}/question4.csv", append = false)
    try {
      // Write header
      writer.writeRow(
        Seq("Passenger 1 ID", "Passenger 2 ID", "Number of Teleports together")
      )
      // Write data
      pairCounts.foreach { case (pair, count) =>
        writer.writeRow(Seq(pair._1.toString, pair._2.toString, count.toString))
      }
    } finally {
      writer.close()
    }
  }
}
