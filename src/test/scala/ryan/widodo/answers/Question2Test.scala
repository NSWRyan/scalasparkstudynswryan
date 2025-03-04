package ryan.widodo.answers

import com.github.tototoshi.csv.CSVReader
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import ryan.widodo.dao.{Teleport, Passenger}

import java.nio.file.{Files, Paths}
import java.sql.Date

class Question2Test extends AnyFunSuite with BeforeAndAfter {
  var tempDir: String = _

  before {
    tempDir = Files.createTempDirectory("test-question2-output").toString
  }

  after {
    // Clean up the temporary directory
    Files
      .walk(Paths.get(tempDir))
      .sorted(java.util.Comparator.reverseOrder())
      .forEach(Files.delete)
  }

  test(
    "question2Outer should correctly join Teleport counts with passenger details and write to CSV"
  ) {
    val defaultDate = Date.valueOf("2010-01-01")
    val Teleports = List(
      Teleport(1, 1001, "us", "zz", defaultDate),
      Teleport(1, 1002, "us", "zz", defaultDate),
      Teleport(1, 1003, "us", "zz", defaultDate),
      Teleport(2, 1003, "us", "zz", defaultDate),
      Teleport(3, 1001, "us", "zz", defaultDate),
      Teleport(3, 1002, "us", "zz", defaultDate),
      Teleport(4, 1003, "us", "zz", defaultDate),
      Teleport(5, 1004, "us", "zz", defaultDate),
      Teleport(6, 1005, "us", "zz", defaultDate)
    )

    val passengers = List(
      Passenger(1, "John", "Doe"),
      Passenger(2, "Jane", "Smith"),
      Passenger(3, "Alice", "Johnson")
    )

    // Execute the function
    Question2.question2Outer(tempDir, Teleports, passengers)

    // Check that the output CSV file exists
    val outputPath = Paths.get(tempDir, "question2Outer.csv")
    assert(Files.exists(outputPath))

    // Read the output CSV file and validate the content
    val reader = CSVReader.open(outputPath.toFile)
    val rows = reader.all()

    // Expected data
    val expectedHeader =
      List("Passenger", "Number of Teleports", "First name", "Last name")
    val expectedData = List(
      List("1", "3", "John", "Doe"),
      List("3", "2", "Alice", "Johnson"),
      List("2", "1", "Jane", "Smith"),
      List("4", "1", "Unknown", "Unknown"),
      List("5", "1", "Unknown", "Unknown"),
      List("6", "1", "Unknown", "Unknown")
    )

    assert(rows.head == expectedHeader)
    assert(rows.tail == expectedData)

    reader.close()
  }

  test(
    "question2Inner should correctly join Teleport counts with passenger details and write to CSV"
  ) {
    val defaultDate = Date.valueOf("2010-01-01")
    val Teleports = List(
      Teleport(1, 1001, "us", "zz", defaultDate),
      Teleport(1, 1002, "us", "zz", defaultDate),
      Teleport(1, 1003, "us", "zz", defaultDate),
      Teleport(2, 1003, "us", "zz", defaultDate),
      Teleport(3, 1001, "us", "zz", defaultDate),
      Teleport(3, 1002, "us", "zz", defaultDate),
      Teleport(4, 1003, "us", "zz", defaultDate),
      Teleport(5, 1004, "us", "zz", defaultDate),
      Teleport(6, 1005, "us", "zz", defaultDate)
    )

    val passengers = List(
      Passenger(1, "John", "Doe"),
      Passenger(2, "Jane", "Smith"),
      Passenger(3, "Alice", "Johnson"),
      Passenger(4, "Bob", "Brown")
    )

    // Execute the function
    Question2.question2Inner(tempDir, Teleports, passengers)

    // Check that the output CSV file exists
    val outputPath = Paths.get(tempDir, "question2Inner.csv")
    assert(Files.exists(outputPath))

    // Read the output CSV file and validate the content
    val reader = CSVReader.open(outputPath.toFile)
    val rows = reader.all()

    // Expected data
    val expectedHeader =
      List("Passenger", "Number of Teleports", "First name", "Last name")
    val expectedData = List(
      List("1", "3", "John", "Doe"),
      List("3", "2", "Alice", "Johnson"),
      List("2", "1", "Jane", "Smith"),
      List("4", "1", "Bob", "Brown")
    )

    assert(rows.head == expectedHeader)
    assert(rows.tail == expectedData)

    reader.close()
  }
}
