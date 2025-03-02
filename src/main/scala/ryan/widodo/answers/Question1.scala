package ryan.widodo.answers

import com.github.tototoshi.csv.CSVWriter
import ryan.widodo.Utils
import ryan.widodo.dao.Teleport

/** Problem: Calculate the monthly Teleport count.
  *
  * Solution: Extract month from date and do group by count on the month.
  */
object Question1 {

  /** Create Teleports per month by grouping by on month.
    */
  def question1(outputDir: String, Teleports: List[Teleport]): Unit = {
    // Scala group by month
    val TeleportsPerMonth: Map[Int, Int] =
      Teleports
        .groupBy(Teleport => Utils.getMonth(Teleport.date))
        // Convert the values to a set of teleportId then count.
        .mapValues(_.map(_.teleportId).toSet.size)
    // Sort by month
    val sortedteleportCountsByMonth: Seq[(Int, Int)] =
      TeleportsPerMonth.toSeq.sortBy(_._1)

    // Write to a new CSV
    val writer: CSVWriter =
      CSVWriter.open(file = s"${outputDir}/question1.csv", append = false)
    try {
      // Write header
      writer.writeRow(Seq("Month", "Number of Teleports"))
      // Write data
      sortedteleportCountsByMonth.foreach { case (month, count) =>
        writer.writeRow(Seq(month.toString, count.toString))
      }
    } finally {
      writer.close()
    }
  }
}
