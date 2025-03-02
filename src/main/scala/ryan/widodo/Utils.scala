package ryan.widodo

import java.nio.file.{Files, Path, Paths}
import java.time.{LocalDate, ZoneId}
import java.time.format.{DateTimeFormatter, ResolverStyle}
import java.util.Calendar
import java.sql.Date

/** A helper object to make the functions reusable.
  */
object Utils {
  private val dateFormat = DateTimeFormatter
    .ofPattern("uuuu-MM-dd")
    .withResolverStyle(ResolverStyle.STRICT)
  private val simpleDateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd")

  /** Translates Java Date to month Int
    *
    * @return
    *   Month Int.
    */
  def getMonth(date: Date): Int = {
    val calendar = Calendar.getInstance()
    calendar.setTime(date)
    calendar.get(Calendar.MONTH) + 1
  }

  /** Parse Int from string for any Int >= 0.
    *
    * @param value
    *   The string to be parsed
    * @return
    *   Int value of the string. -1 if failed.
    */
  def parseInt(value: String): Int = {
    try {
      value.toInt
    } catch {
      case _: NumberFormatException =>
        -1
    }
  }

  /** Parse Date from string for any date > 1970
    *
    * @param value
    *   The string to be parsed
    * @return
    *   Return 1970-01-01 if failed, so we can filter it out later.
    */
  def parseDate(value: String): Date = {
    try {
      val localDate = LocalDate.parse(value, dateFormat)
      Date.valueOf(localDate)
    } catch {
      case _: Throwable => {
        Date.valueOf("1970-01-01")
      }
    }
  }

  /** Convert Date to string for any date
    *
    * @param value
    *   The string to be parsed
    * @return
    *   The String version of date according to simpleDateFormat in Utils.
    */
  def dateToString(value: Date): String = {
    simpleDateFormat.format(value)
  }

  /** Create a directory if not exist yet.
    */
  def createDirIfNotExist(outputDir: String): Unit = {
    try {
      // Output dir management
      val path: Path = Paths.get(outputDir)
      if (!Files.exists(path)) {
        Files.createDirectories(path)

      }
    } catch {
      case e: Exception =>
        println(
          s"An exception was caught during output dir creation ${e.getMessage}"
        )
    }
  }
}
