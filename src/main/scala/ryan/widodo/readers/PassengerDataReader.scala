package ryan.widodo.readers

import com.github.tototoshi.csv._
import ryan.widodo.Utils
import ryan.widodo.dao.Passenger

object PassengerDataReader {

  /** Parse the CSV row into Teleport object
    * @param row
    *   The CSV row
    * @return
    *   Passenger object.
    */
  private def parsePassenger(row: Map[String, String]): Passenger = {
    Passenger(
      Utils.parseInt(row("passengerId")),
      firstName = row("firstName"),
      lastName = row("lastName")
    )
  }

  /** Parse the CSV rows into passengers data
    * @param filePath
    *   A map of the column name => value
    * @return
    *   A list of Passenger objects.
    */
  def readPassengers(filePath: String): List[Passenger] = {
    val reader = CSVReader.open(filePath)
    val Teleports = reader.allWithHeaders().map { row =>
      parsePassenger(row)
    }
    reader.close()
    Teleports
  }
}
