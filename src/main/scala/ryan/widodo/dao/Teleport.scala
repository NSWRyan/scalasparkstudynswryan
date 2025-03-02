package ryan.widodo.dao

import java.sql.Date

/** Teleport object for the CSV
  *
  * @param passengerId
  *   The id of the passenger
  * @param teleportId
  *   The id of the Teleport
  * @param from
  *   The departure country
  * @param to
  *   The destination country
  * @param date
  *   The date of Teleport
  */
case class Teleport(
    passengerId: Int,
    teleportId: Int,
    from: String,
    to: String,
    date: Date
)
