package ryan.widodo.answers

import com.github.tototoshi.csv.CSVWriter
import ryan.widodo.dao.Teleport
import ryan.widodo.answers.Question3Helper.{countriesChain, longestRunNoZZ}
import scala.collection.mutable.ListBuffer

/** Problem: We need to return the longest length of the subsequence of visited
  * countries. This presents several challenges to solve.
  *
  *   1. We need to get the list of Teleports for each passengerId and sort it
  *      based on the date.
  *   1. We need to process the list of countries.There are some cases that we
  *      need to cover here,
  *      {{{
  *      Case 1:
  *      ZZ => FR, FR => US.
  *      In this case the list should be
  *      ZZ => FR => US.
  *      Case 2:
  *      ZZ => FR, JA => US.
  *      In this case the list should be
  *      ZZ => FR => JA => US.
  *      }}}
  *   - Solution: We can create a mutable list and check the tail with the
  *     country to be inserted, to be sure that we are dealing with distinct
  *     countries in the list.
  *   1. We need to count the length of the longest subsequence.
  *      - Solution:
  *        - In this case, we can just iterate the list of countries we created
  *          in #2.
  *        - For each iteration we increase the counter by 1. If we are getting
  *          ZZ, then reset the counter to 0.
  *        - We compare the current max with the counter value for each
  *          iteration.
  */
object Question3 {

  /** The main answer function.
    *
    * @param outputDir
    *   The output dir to write the CSV into.
    * @param Teleports
    *   The list of Teleport object to process.
    */
  def question3(outputDir: String, Teleports: List[Teleport]): Unit = {
    // First group by the passengerId to get the list of Teleport objects for each passengerId.
    val passengersteleportsData =
      Teleports.groupBy(_.passengerId)

    // We do two steps here.
    // First, get the list of visited countries sorted by date.
    // Second, We find the longest subsequence of countries without ZZ.
    val visitedCountriesByPassenger: Map[Int, Int] =
      passengersteleportsData.map { case (passengerId, teleportsData) =>
        val longestRun = longestRunNoZZ(countriesChain(teleportsData))
        passengerId -> longestRun
      }
    // Sort in descending order.
    val visitedCountriesByPassengerSorted =
      visitedCountriesByPassenger.toSeq
        .sortBy(passengerRun => (-passengerRun._2, passengerRun._1))

    // Write to a new CSV
    val writer: CSVWriter =
      CSVWriter.open(file = s"$outputDir/question3.csv", append = false)
    try {
      // Write header
      writer.writeRow(Seq("Passenger ID", "Longest Run"))
      // Write data
      visitedCountriesByPassengerSorted.foreach { case (passengerId, count) =>
        writer.writeRow(Seq(passengerId.toString, count.toString))
      }
    } finally {
      writer.close()
    }
  }
}
