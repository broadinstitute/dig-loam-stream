package loamstream.loam.intake

/**
 * @author clint
 * Jul 20, 2020
 */
final case class CsvProcessingException(message: String, row: CsvRow, cause: Throwable) extends 
    Exception(message, cause)
