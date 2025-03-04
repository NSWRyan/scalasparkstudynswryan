package ryan.widodo

import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.{Files, Path, Paths}
import java.time.{LocalDate, ZoneId}
import java.util.Calendar
import java.sql.Date

class UtilsTest extends AnyFunSuite {

  test("getMonth should return the correct month for a given date") {
    val calendar = Calendar.getInstance()

    // January (0 in Calendar, so should return 1)
    val januaryDate: java.sql.Date = java.sql.Date.valueOf("2024-01-15")
    assert(Utils.getMonth(januaryDate) == 1)

    // May (4 in Calendar, so should return 5)
    val mayDate: java.sql.Date = java.sql.Date.valueOf("2024-05-15")
    assert(Utils.getMonth(mayDate) == 5)

    // December (11 in Calendar, so should return 12)
    calendar.set(2024, Calendar.DECEMBER, 15)
    val decemberDate: java.sql.Date = java.sql.Date.valueOf("2024-12-15")
    assert(Utils.getMonth(decemberDate) == 12)
  }

  test(
    "parseInt should return the correct integer when a valid string is provided"
  ) {
    assert(Utils.parseInt("123") == 123)
    assert(Utils.parseInt("0") == 0)
    assert(Utils.parseInt("-456") == -456)
  }

  test("parseInt should return -1 when an invalid string is provided") {
    assert(Utils.parseInt("abc") == -1)
    assert(Utils.parseInt("12.34") == -1)
    assert(Utils.parseInt("") == -1)
    assert(Utils.parseInt(" ") == -1)
    assert(Utils.parseInt("123abc") == -1)
  }

  test("parseInt should return -1 when null is provided") {
    assert(Utils.parseInt(null) == -1)
  }

  test(
    "parseDate should return the correct Date when a valid string is provided"
  ) {
    val expectedDate1 = Date.valueOf("2024-08-15")
    assert(Utils.parseDate("2024-08-15") == expectedDate1)

    val expectedDate2 = Date.valueOf("2000-01-01")
    assert(Utils.parseDate("2000-01-01") == expectedDate2)
  }

  test(
    "parseDate should return default date (1970-01-01) when an invalid string is provided"
  ) {
    val defaultDate = Date.valueOf("1970-01-01")

    assert(Utils.parseDate("invalid-date") == defaultDate)
    assert(Utils.parseDate("2024-02-31") == defaultDate) // Invalid date
    assert(Utils.parseDate("") == defaultDate)
    assert(Utils.parseDate(" ") == defaultDate)
  }

  test(
    "parseDate should return default date (1970-01-01) when null is provided"
  ) {
    val defaultLocalDate = LocalDate.parse("1970-01-01")
    val defaultDate = Date.valueOf(defaultLocalDate)
    assert(Utils.parseDate(null) == defaultDate)
  }

  test(
    "dateToString should return the correct string representation of a Date"
  ) {
    val date1 = Date.valueOf("2024-08-15")
    assert(Utils.dateToString(date1) == "2024-08-15")

    val date2 = Date.valueOf("2000-01-01")
    assert(Utils.dateToString(date2) == "2000-01-01")

    val date3 = Date.valueOf("1970-01-01")
    assert(Utils.dateToString(date3) == "1970-01-01")
  }

  test("dateToString should throw NullPointerException when null is provided") {
    assertThrows[NullPointerException] {
      Utils.dateToString(null)
    }
  }

  test("createDirIfNotExist should create the directory if it does not exist") {
    val testDir = "test-output-dir"

    // Ensure the directory doesn't exist before the test
    val path: Path = Paths.get(testDir)
    if (Files.exists(path)) {
      Files.delete(path)
    }

    assert(!Files.exists(path)) // Directory should not exist initially

    Utils.createDirIfNotExist(testDir)

    assert(Files.exists(path)) // Directory should be created

    // Clean up by deleting the directory after the test
    Files.delete(path)
  }

  test(
    "createDirIfNotExist should not throw an exception if the directory already exists"
  ) {
    val testDir = "test-existing-dir"

    // Ensure the directory exists before the test
    val path: Path = Paths.get(testDir)
    if (!Files.exists(path)) {
      Files.createDirectory(path)
    }

    assert(Files.exists(path)) // Directory should exist

    Utils.createDirIfNotExist(testDir)

    assert(Files.exists(path)) // Directory should still exist

    // Clean up by deleting the directory after the test
    Files.delete(path)
  }

  test(
    "createDirIfNotExist should handle exceptions during directory creation gracefully"
  ) {
    def getInvalidDir: String = {
      val osName = System.getProperty("os.name").toLowerCase
      if (osName.toLowerCase.contains("windows")) {
        "asd:\\"
      } else {
        "/invalid-directory/test-dir"
      }
    }
    val invalidDir = getInvalidDir // Assuming this is an invalid path

    // Attempt to create directory and catch the printed exception
    val stream = new java.io.ByteArrayOutputStream()
    Console.withOut(stream) {
      Utils.createDirIfNotExist(invalidDir)
    }

    assert(
      stream.toString.contains(
        "An exception was caught during output dir creation"
      )
    )
  }
}
