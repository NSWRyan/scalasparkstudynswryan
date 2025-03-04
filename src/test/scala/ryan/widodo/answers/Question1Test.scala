package ryan.widodo.answers

import com.github.tototoshi.csv.CSVReader
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import ryan.widodo.Utils
import ryan.widodo.dao.Teleport

import java.nio.file.{Files, Paths}

class Question1Test extends AnyFunSuite with BeforeAndAfter {
  var tempDir: String = _

  before {
    tempDir = Files.createTempDirectory("test-question1-output").toString
  }

  after {
    // Clean up the temporary directory
    Files
      .walk(Paths.get(tempDir))
      .sorted(java.util.Comparator.reverseOrder())
      .forEach(Files.delete)
  }

  test(
    "question1 should correctly count and sort Teleports by month and write to CSV"
  ) {
    val Teleports = List(
      Teleport(1, 1001, "id", "us", Utils.parseDate("2024-01-15")),
      Teleport(2, 1002, "us", "zz", Utils.parseDate("2024-01-20")),
      Teleport(3, 1003, "id", "us", Utils.parseDate("2024-02-10")),
      Teleport(4, 1004, "id", "zz", Utils.parseDate("2024-02-15")),
      Teleport(5, 1005, "us", "zz", Utils.parseDate("2024-02-20")),
      Teleport(6, 1006, "zz", "id", Utils.parseDate("2024-03-10"))
    )

    // Execute the function
    Question1.question1(tempDir, Teleports)

    // Check that the output CSV file exists
    val outputPath = Paths.get(tempDir, "question1.csv")
    assert(Files.exists(outputPath))

    // Read the output CSV file and validate the content
    val reader = CSVReader.open(outputPath.toFile)
    val rows = reader.all()

    // Expected data
    val expectedHeader = List("Month", "Number of Teleports")
    val expectedData = List(
      List("1", "2"), // January
      List("2", "3"), // February
      List("3", "1") // March
    )

    assert(rows.head == expectedHeader)
    assert(rows.tail == expectedData)

    reader.close()
  }
}
