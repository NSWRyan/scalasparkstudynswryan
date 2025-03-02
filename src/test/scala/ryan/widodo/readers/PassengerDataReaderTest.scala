package ryan.widodo.readers

import com.github.tototoshi.csv.CSVWriter
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import ryan.widodo.Utils
import ryan.widodo.dao.{Teleport, Passenger}

import java.nio.file.{Files, Paths}

class PassengerDataReaderTest extends AnyFunSuite with BeforeAndAfter {
  var tempDir: String  = _
  var tempFile: String = _

  before {
    tempDir = Files.createTempDirectory("test-read-passengers").toString
    tempFile = s"$tempDir/test-passengers.csv"

    // Sample CSV data
    val csvData = Seq(
      Seq("passengerId", "firstName", "lastName"),
      Seq("1", "John", "Doe"),
      Seq("2", "Jane", "Smith"),
      Seq("3", "Alice", "Johnson")
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

  test(
    "readPassengers should correctly parse CSV rows into Passenger objects"
  ) {
    val expectedPassengers = List(
      Passenger(1, "John", "Doe"),
      Passenger(2, "Jane", "Smith"),
      Passenger(3, "Alice", "Johnson")
    )

    // Execute the function
    val passengers = PassengerDataReader.readPassengers(tempFile)

    // Validate the parsed Passenger objects
    assert(passengers == expectedPassengers)
  }
}
