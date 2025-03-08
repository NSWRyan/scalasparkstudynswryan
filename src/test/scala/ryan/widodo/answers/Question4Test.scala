package ryan.widodo.answers

import com.github.tototoshi.csv.CSVReader
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import ryan.widodo.dao.Teleport

import java.nio.file.{Files, Paths}
import java.sql.Date

class Question4Test
    extends AnyFunSuite
    with Matchers
    with BeforeAndAfter
    with MockFactory {
  var tempDir: String = _

  before {
    tempDir = Files.createTempDirectory("test-question4-output").toString
  }

  after {
    // Clean up the temporary directory
    Files
      .walk(Paths.get(tempDir))
      .sorted(java.util.Comparator.reverseOrder())
      .forEach(Files.delete)
  }

  test("question4 should create a CSV with correct pairings and counts") {
    val dummyDate = Date.valueOf("2010-01-01")
    val teleports = List(
      Teleport(1, 100, "", "", dummyDate),
      Teleport(1, 101, "", "", dummyDate),
      Teleport(1, 102, "", "", dummyDate),
      Teleport(1, 103, "", "", dummyDate),
      Teleport(2, 100, "", "", dummyDate),
      Teleport(2, 101, "", "", dummyDate),
      Teleport(2, 102, "", "", dummyDate),
      Teleport(2, 103, "", "", dummyDate),
      Teleport(3, 100, "", "", dummyDate),
      Teleport(3, 101, "", "", dummyDate),
      Teleport(3, 102, "", "", dummyDate),
      Teleport(3, 103, "", "", dummyDate),
      Teleport(4, 100, "", "", dummyDate),
      Teleport(4, 101, "", "", dummyDate),
      Teleport(4, 102, "", "", dummyDate),
      Teleport(4, 103, "", "", dummyDate),
      Teleport(5, 100, "", "", dummyDate),
      Teleport(5, 101, "", "", dummyDate),
      Teleport(5, 102, "", "", dummyDate)
    )

    // Execute the function
    Question4.question4(tempDir, teleports)

    // Check that the output CSV file exists
    val outputPath = Paths.get(tempDir, "question4.csv")
    assert(Files.exists(outputPath))

    // Read the output CSV file and validate the content
    val reader = CSVReader.open(outputPath.toFile)
    val rows = reader.all()

    // Expected data
    val expectedHeader =
      List("Passenger 1 ID", "Passenger 2 ID", "Number of Teleports together")
    val expectedData = List(
      List("1", "2", "4"),
      List("1", "3", "4"),
      List("1", "4", "4"),
      List("2", "3", "4"),
      List("2", "4", "4"),
      List("3", "4", "4")
    )

    assert(rows.head == expectedHeader)
    assert(rows.tail == expectedData)

    reader.close()
  }
}
