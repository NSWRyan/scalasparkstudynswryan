package ryan.widodo.answers

import com.github.tototoshi.csv.CSVReader
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import ryan.widodo.Utils
import ryan.widodo.dao.{Teleport, Passenger}

import java.nio.file.{Files, Paths}

class Question3Test extends AnyFunSuite with BeforeAndAfter {
  var tempDir: String = _

  before {
    tempDir = Files.createTempDirectory("test-question3-output").toString
  }

  after {
    // Clean up the temporary directory
    Files
      .walk(Paths.get(tempDir))
      .sorted(java.util.Comparator.reverseOrder())
      .forEach(Files.delete)
  }

  test(
    "question3 should correctly calculate the longest run of countries visited without ZZ and write to CSV"
  ) {
    val Teleports = List(
      Teleport(1, 1001, "us", "ca", Utils.parseDate("2023-01-01")),
      Teleport(1, 1003, "fr", "zz", Utils.parseDate("2023-01-20")),
      Teleport(1, 1002, "ca", "fr", Utils.parseDate("2023-01-10")),
      Teleport(1, 1004, "zz", "ge", Utils.parseDate("2023-01-30")),
      Teleport(1, 1005, "ge", "sp", Utils.parseDate("2023-02-10")),
      Teleport(2, 1006, "us", "me", Utils.parseDate("2023-01-05")),
      Teleport(2, 1007, "me", "br", Utils.parseDate("2023-01-15")),
      Teleport(2, 1008, "br", "ar", Utils.parseDate("2023-01-25"))
    )

    // Execute the function
    Question3.question3(tempDir, Teleports)

    // Check that the output CSV file exists
    val outputPath = Paths.get(tempDir, "question3.csv")
    assert(Files.exists(outputPath))

    // Read the output CSV file and validate the content
    val reader = CSVReader.open(outputPath.toFile)
    val rows = reader.all()

    // Expected data
    val expectedHeader = List("Passenger ID", "Longest Run")
    val expectedData = List(
      List("2", "4"), // USA -> Mexico -> Brazil -> Argentina
      // USA -> Canada -> French
      // Germany -> Spain (ZZ resets the count)
      List("1", "3")
    )

    assert(rows.head == expectedHeader)
    assert(rows.tail == expectedData)

    reader.close()
  }

}
