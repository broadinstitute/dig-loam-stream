package loamstream.loam.intake.metrics

/**
 * @author clint
 * Mar 31, 2020
 */
trait HttpClient {
  def get(url: String): Either[String, String]
}
