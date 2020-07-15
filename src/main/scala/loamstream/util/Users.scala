package loamstream.util

/**
 * @author clint
 * Jul 15, 2020
 */
object Users {
  def currentUser: String = System.getProperty("user.name")
}
