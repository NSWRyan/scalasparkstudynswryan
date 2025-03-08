package ryan.widodo.answers

import com.github.tototoshi.csv.CSVReader
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import ryan.widodo.Utils
import ryan.widodo.dao.Teleport

import java.nio.file.{Files, Paths}

class Question3HelperTest extends AnyFunSuite {
  var tempDir: String = _

  test(
    "countriesChain should return an empty list when the input is an empty list"
  ) {
    assert(Question3Helper.countriesChain(Nil) === Nil)
  }

  test(
    "countriesChain should return the correct chain of countries for a list of Teleports"
  ) {
    val teleports = List(
      Teleport(1, 1, "us", "zz", Utils.parseDate("2023-07-10")),
      Teleport(2, 2, "zz", "ge", Utils.parseDate("2023-07-11")),
      Teleport(3, 3, "ge", "fr", Utils.parseDate("2023-07-12")),
      Teleport(4, 4, "fr", "sp", Utils.parseDate("2023-07-13"))
    )

    val expected = List("us", "zz", "ge", "fr", "sp")
    assert(Question3Helper.countriesChain(teleports) === expected)
  }

  test(
    "countriesChain should handle Teleports with the same from and to countries correctly"
  ) {
    val teleports = List(
      Teleport(1, 1, "us", "zz", Utils.parseDate("2023-07-10")),
      Teleport(2, 2, "zz", "zz", Utils.parseDate("2023-07-11")),
      Teleport(3, 3, "zz", "ge", Utils.parseDate("2023-07-12"))
    )

    val expected = List("us", "zz", "ge")
    assert(Question3Helper.countriesChain(teleports) === expected)
  }

  test("countriesChain should sort the Teleports by date before processing") {
    val teleports = List(
      Teleport(1, 2, "zz", "ge", Utils.parseDate("2023-07-12")),
      Teleport(2, 1, "us", "zz", Utils.parseDate("2023-07-10")),
      Teleport(3, 3, "ge", "fr", Utils.parseDate("2023-07-14"))
    )

    val expected = List("us", "zz", "ge", "fr")
    assert(Question3Helper.countriesChain(teleports) === expected)
  }

  test("longestRunNoZZ should return 0 for an empty list") {
    assert(Question3Helper.longestRunNoZZ(Nil) === 0)
  }

  test(
    "longestRunNoZZ should return the correct run length when there are no 'ZZ' countries"
  ) {
    val countries = List("us", "ca", "ge", "fr")
    assert(Question3Helper.longestRunNoZZ(countries) === 4)
  }

  test(
    "longestRunNoZZ should return the correct run length when 'ZZ' is present"
  ) {
    val countries = List("us", "ca", "zz", "ge", "fr")
    assert(Question3Helper.longestRunNoZZ(countries) === 2)
  }

  test(
    "longestRunNoZZ should return the correct run length when 'ZZ' is at the start"
  ) {
    val countries = List("zz", "us", "ca", "ge")
    assert(Question3Helper.longestRunNoZZ(countries) === 3)
  }

  test(
    "longestRunNoZZ should return the correct run length when 'ZZ' is at the end"
  ) {
    val countries = List("us", "ca", "ge", "zz")
    assert(Question3Helper.longestRunNoZZ(countries) === 3)
  }

  test("longestRunNoZZ should handle multiple occurrences of 'ZZ' correctly") {
    val countries = List("us", "zz", "ge", "zz", "fr", "sp")
    assert(Question3Helper.longestRunNoZZ(countries) === 2)
  }

  test("longestRunNoZZ should be case-insensitive when identifying 'ZZ'") {
    val countries = List("us", "ca", "zz", "ge", "zz", "fr")
    assert(Question3Helper.longestRunNoZZ(countries) === 2)
  }
}
