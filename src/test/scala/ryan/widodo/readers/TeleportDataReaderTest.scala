package ryan.widodo.readers

import com.github.tototoshi.csv.CSVWriter
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import ryan.widodo.Utils
import ryan.widodo.dao.Teleport

import java.nio.file.{Files, Paths}

class TeleportDataReaderTest extends AnyFunSuite with BeforeAndAfter {
  var tempDir: String = _
  var tempFile: String = _

  before {
    tempDir = Files.createTempDirectory("test-read-Teleports").toString
    tempFile = s"$tempDir/test-Teleports.csv"

    // Sample CSV data
    val csvData = Seq(
      Seq("passengerId", "teleportId", "from", "to", "date"),
      Seq("1", "1001", "a", "b", "2023-01-02"),
      Seq("2", "1002", "b", "c", "2023-01-10"),
      Seq("3", "1003", "c", "d", "2023-01-20")
    )

    // Write the sample CSV data to file
    val writer = CSVWriter.open(tempFile)
    writer.writeAll(csvData)
    writer.close()
  }

  after {
    // Clean up the temporary directory and file
    Files
      .walk(Paths.get(tempDir))
      .sorted(java.util.Comparator.reverseOrder())
      .forEach(Files.delete)
  }

  test("readTeleports should correctly parse CSV rows into Teleport objects") {
    val expectedTeleports = List(
      Teleport(
        1,
        1001,
        "a",
        "b",
        Utils.parseDate("2023-01-02")
      ),
      Teleport(
        2,
        1002,
        "b",
        "c",
        Utils.parseDate("2023-01-10")
      ),
      Teleport(
        3,
        1003,
        "c",
        "d",
        Utils.parseDate("2023-01-20")
      )
    )

    // Execute the function
    val teleports = teleportDataReader.readTeleports(tempFile)

    // Validate the parsed Teleport objects
    assert(teleports == expectedTeleports)
  }
}
