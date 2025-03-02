package ryan.widodo.readers

import com.github.tototoshi.csv._
import ryan.widodo.Utils
import ryan.widodo.dao.Teleport

object teleportDataReader {

  /** Parse the CSV row into Teleport object.
    *
    * @param row
    *   The CSV row
    * @return
    *   Teleport object.
    */
  private def parseTeleport(row: Map[String, String]): Teleport = {
    Teleport(
      Utils.parseInt(row("passengerId")),
      Utils.parseInt(row("teleportId")),
      from = row("from"),
      to = row("to"),
      date = Utils.parseDate(row("date"))
    )
  }

  /** Parse the CSV rows into teleports data
    *
    * @param filePath
    *   A map of the column name => value
    * @return
    *   A list of Teleport objects.
    */
  def readTeleports(filePath: String): List[Teleport] = {
    val reader = CSVReader.open(filePath)
    val teleports = reader.allWithHeaders().map { row =>
      parseTeleport(row)
    }
    reader.close()
    teleports
  }
}
