package loamstream.loam.intake

/**
 * @author clint
 * Jul 20, 2020
 */
final case class CsvProcessingException(message: String, row: DataRow, cause: Throwable = null) extends 
    Exception(message, cause)
