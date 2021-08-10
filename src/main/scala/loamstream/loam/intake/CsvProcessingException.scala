package loamstream.loam.intake

/**
 * @author clint
 * Jul 20, 2020
 */
final class CsvProcessingException(
    message: => String, 
    val row: DataRow,
    val from: ColumnExpr[_],
    val cause: Throwable = null) extends Exception(cause) { //scalastyle:ignore null

  override def getMessage: String = message
}
