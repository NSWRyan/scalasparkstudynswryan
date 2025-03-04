package ryan.widodo.answers

import com.github.tototoshi.csv.CSVReader
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import ryan.widodo.Utils
import ryan.widodo.dao.Teleport

import java.nio.file.{Files, Paths}
import java.util.Date

class Question5Test
    extends AnyFunSuite
    with Matchers
    with BeforeAndAfter
    with MockFactory {
  var tempDir: String = _

  before {
    tempDir = Files.createTempDirectory("test-question5-output").toString
  }

  after {
    // Clean up the temporary directory
    Files
      .walk(Paths.get(tempDir))
      .sorted(java.util.Comparator.reverseOrder())
      .forEach(Files.delete)
  }

  test(
    "teleportedTogether should correctly calculate the number of Teleports together and write to CSV"
  ) {
    val fromDate = Utils.parseDate("2024-01-01")
    val toDate = Utils.parseDate("2024-01-06")

    val Teleports = List(
      Teleport(1, 100, "", "", Utils.parseDate("2024-01-01")),
      Teleport(1, 101, "", "", Utils.parseDate("2024-01-02")),
      Teleport(1, 102, "", "", Utils.parseDate("2024-01-03")),
      Teleport(1, 103, "", "", Utils.parseDate("2024-01-04")),
      Teleport(1, 104, "", "", Utils.parseDate("2024-01-05")),
      Teleport(1, 105, "", "", Utils.parseDate("2024-01-06")),
      // 106 should be filtered out
      Teleport(1, 106, "", "", Utils.parseDate("2024-01-07")),
      Teleport(2, 100, "", "", Utils.parseDate("2024-01-01")),
      Teleport(2, 101, "", "", Utils.parseDate("2024-01-02")),
      Teleport(2, 102, "", "", Utils.parseDate("2024-01-03")),
      Teleport(2, 103, "", "", Utils.parseDate("2024-01-04")),
      Teleport(3, 100, "", "", Utils.parseDate("2024-01-01")),
      Teleport(3, 101, "", "", Utils.parseDate("2024-01-02")),
      Teleport(3, 102, "", "", Utils.parseDate("2024-01-03")),
      Teleport(3, 104, "", "", Utils.parseDate("2024-01-05")),
      Teleport(4, 100, "", "", Utils.parseDate("2024-01-01")),
      Teleport(4, 101, "", "", Utils.parseDate("2024-01-02")),
      Teleport(4, 102, "", "", Utils.parseDate("2024-01-03")),
      Teleport(4, 105, "", "", Utils.parseDate("2024-01-06")),
      Teleport(5, 100, "", "", Utils.parseDate("2024-01-01")),
      Teleport(5, 101, "", "", Utils.parseDate("2024-01-02")),
      Teleport(5, 102, "", "", Utils.parseDate("2024-01-03")),
      // 106 should be filtered out
      Teleport(5, 106, "", "", Utils.parseDate("2024-01-07"))
    )

    // Execute the function
    Question5.teleportedTogether(tempDir, Teleports, 4, fromDate, toDate)

    // Check that the output CSV file exists
    val outputPath = Paths.get(tempDir, "question5.csv")
    assert(Files.exists(outputPath))

    // Read the output CSV file and validate the content
    val reader = CSVReader.open(outputPath.toFile)
    val rows = reader.all()

    // Expected data
    val expectedHeader = List(
      "Passenger 1 ID",
      "Passenger 2 ID",
      "Number of Teleports together",
      "From",
      "To"
    )
    val expectedData = List(
      List("1", "2", "4", "2024-01-01", "2024-01-04"),
      List("1", "3", "4", "2024-01-01", "2024-01-05"),
      List("1", "4", "4", "2024-01-01", "2024-01-06")
    )

    assert(rows.head == expectedHeader)
    assert(rows.tail == expectedData)

    reader.close()
  }
}
