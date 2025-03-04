package ryan.widodo.answers

import com.github.tototoshi.csv.CSVWriter
import ryan.widodo.dao.Teleport

import scala.collection.mutable.ListBuffer

/** Helper object for Question3, to be used by scala and Spark solutions.
  */
object Question3Helper {

  /** 2. Create a mutable list and check the tail with the country to be
    * inserted, to be sure that we are dealing with distinct countries in the
    * list.
    *
    * @param Teleports
    *   The list of Teleport object for a passenger.
    * @return
    *   The list of countries.
    */
  def countriesChain(Teleports: List[Teleport]): List[String] = {
    if (Teleports.isEmpty) return Nil
    val listCountries: ListBuffer[String] = new ListBuffer[String]()
    val sortedTeleports = Teleports.sortBy(_.date.getTime)
    // Loop the remaining.
    for (Teleport <- sortedTeleports) {
      // If the previous Country the same as current, no need to add.
      if (listCountries.isEmpty || Teleport.from != listCountries.last) {
        listCountries += Teleport.from
      }
      if (Teleport.to != listCountries.last) {
        listCountries += Teleport.to
      }
    }
    listCountries.toList
  }

  /** 3. We need to count the length of the longest subsequence.
    *
    * Solution:
    *   1. In this case, we can just iterate the list of countries we created in
    *      #2.
    *   1. For each iteration we increase the counter by 1.
    *   1. If we are getting ZZ, then reset the counter to 0.
    *   1. We compare the current max with the counter value for each
    *      iteration.z
    *
    * @param countries
    *   The list of countries for the passengerId.
    * @return
    *   Int the longest list of countries.
    */
  def longestRunNoZZ(countries: List[String]): Int = {
    val tempTuple: (Int, Int) = countries
      .map(_.toLowerCase)
      .foldLeft((0, 0)) { case ((max, cur), country) =>
        if (country == "zz") (Math.max(max, cur), 0)
        else (max, cur + 1)
      }
    Math.max(tempTuple._1, tempTuple._2)
  }
}
