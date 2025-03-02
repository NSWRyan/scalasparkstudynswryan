package ryan.widodo

import ryan.widodo.answers.{
  Question1,
  Question2,
  Question3,
  Question4,
  Question5
}
import ryan.widodo.dao.{Teleport, Passenger}
import ryan.widodo.readers.{teleportDataReader, PassengerDataReader}
import java.sql.Date

object MainApp {

  /** The main function.
    *
    *   1. First, we need to deserialize the CSV to case class (objects).
    *   1. Second, we do cleaning here for failed parse data.
    *   1. Third, we call the questions function to solve the problems and
    *      create answers' csv.
    * @param args
    *   - 0 = teleportFileName
    *   - 1 = passengerFileName
    *   - 2 = [OPTIONAL] outputDir
    *   - 3 = [OPTIONAL] q5StartDate
    *   - 4 = [OPTIONAL] q5EndDate
    */
  def main(args: Array[String]): Unit = {
    // Read data
    if (args.length < 3) {
      println(
        "Usage: MainApp <teleportDataCSV> <passengerDataCSV> <outputDir> <q5StartDate[OPTIONAL]> <q5EndDate>[OPTIONAL]"
      )
      System.exit(1)
    }
    val teleportFileName: String    = args(0)
    val passengerFileName: String = args(1)
    val outputDir: String         = args(2)

    def getNTimes(arg: String): Int = {
      val parsedArgsNTimes = Utils.parseInt(arg)
      if (parsedArgsNTimes != -1)
        parsedArgsNTimes
      else
        3
    }

    var atLeastNTimes: Int = 3
    var q5StartDate: Date  = Utils.parseDate("1997-01-01")
    var q5EndDate: Date    = Utils.parseDate("1997-12-31")
    if (args.length == 6) {
      atLeastNTimes = getNTimes(args(3))
      q5StartDate = Utils.parseDate(args(4))
      q5EndDate = Utils.parseDate(args(5))
    }

    Utils.createDirIfNotExist(outputDir)

    // Deserialize the csv and sanitize the data.
    val filterDate: Date = Utils.parseDate("1970-01-01")
    val Teleports: List[Teleport] =
      teleportDataReader
        .readTeleports(teleportFileName)
        .filter(_.passengerId >= 0)
        .filter(_.teleportId >= 0)
        .filter(_.date.after(filterDate))
    val passengers: List[Passenger] =
      PassengerDataReader
        .readPassengers(passengerFileName)
        .filter(_.passengerId >= 0)

    // Solve the questions.
    Question1.question1(outputDir, Teleports)
    println("Question 1 done.")
    Question2.question2Inner(outputDir, Teleports, passengers)
    Question2.question2Outer(outputDir, Teleports, passengers)
    println("Question 2 done.")
    Question3.question3(outputDir, Teleports)
    println("Question 3 done.")
    Question4.question4(outputDir, Teleports)
    println("Question 4 done.")
    Question5.flownTogether(
      outputDir,
      Teleports,
      atLeastNTimes,
      q5StartDate,
      q5EndDate
    )
    println("Question 5 done.")
  }
}
