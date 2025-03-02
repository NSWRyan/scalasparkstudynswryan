package ryan.widodo.dao

/** Teleport object for the CSV
  *
  * @param passengerId
  *   The id of the passenger
  * @param firstName
  *   The id of the Teleport
  * @param lastName
  *   The departure country
  */
case class Passenger(
    passengerId: Int,
    firstName: String,
    lastName: String
)
