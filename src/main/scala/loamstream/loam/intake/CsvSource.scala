package loamstream.loam.intake

import org.apache.commons.csv.CSVRecord
import org.apache.commons.csv.CSVFormat
import java.nio.file.Path
import java.io.FileReader
import org.apache.commons.csv.CSVParser
import loamstream.util.TakesEndingActionIterator
import loamstream.util.Throwables
import loamstream.util.Loggable
import loamstream.util.BashScript
import java.nio.file.Paths
import java.io.BufferedReader
import java.io.Reader
import java.io.InputStreamReader

/**
 * @author clint
 * Dec 17, 2019
 */
sealed trait CsvSource {
  def records: Iterator[CSVRecord]
}
  
object CsvSource extends Loggable {
  
  object Defaults {
    val tabDelimitedWithHeaderCsvFormat: CSVFormat = CSVFormat.DEFAULT.withDelimiter('\t').withFirstRecordAsHeader
  }
  
  final case class FromFile(
      path: Path, 
      csvFormat: CSVFormat = Defaults.tabDelimitedWithHeaderCsvFormat) extends CsvSource {
    
    override def records: Iterator[CSVRecord] = toCsvRecordIterator(new FileReader(path.toFile), csvFormat)
  }
  
  final case class FromCommand(
      command: String,
      csvFormat: CSVFormat = Defaults.tabDelimitedWithHeaderCsvFormat,
      workDir: Path = Paths.get(".")) extends CsvSource {
    
    override def records: Iterator[CSVRecord] = {
      val bashScriptForCommand = BashScript.fromCommandLineString(command)
      
      val processBuilder = new java.lang.ProcessBuilder(bashScriptForCommand.commandTokens: _*)
      
      val process = processBuilder.start()
            
      toCsvRecordIterator(
          new InputStreamReader(process.getInputStream), 
          csvFormat)
    }
  }
  
  private def toCsvRecordIterator(reader: Reader, csvFormat: CSVFormat): Iterator[CSVRecord] = {
    import scala.collection.JavaConverters._
      
    val parser = new CSVParser(reader, csvFormat)
    
    val iterator = parser.iterator.asScala
    
    TakesEndingActionIterator(iterator) {
      Throwables.quietly("Closing CSV parser")(parser.close())
      Throwables.quietly("Closing underlying reader")(reader.close())
    }
  }
}
