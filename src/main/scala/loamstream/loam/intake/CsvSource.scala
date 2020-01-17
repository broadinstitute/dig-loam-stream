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
import loamstream.model.jobs.commandline.HasCommandLine

/**
 * @author clint
 * Dec 17, 2019
 */
sealed trait CsvSource {
  def records: Iterator[CsvRow]
  
  def filter(p: RowPredicate): CsvSource = fromCombinator(_.filter(p))
  
  def filterNot(p: RowPredicate): CsvSource = fromCombinator(_.filterNot(p))
  
  private final def addSourceTo(columnDef: UnsourcedColumnDef): SourcedColumnDef = SourcedColumnDef(columnDef, this) 
  
  final def producing(columnDef: UnsourcedColumnDef): SourcedColumnDef = addSourceTo(columnDef)
  
  final def producing(columnDefs: Seq[UnsourcedColumnDef]): Seq[SourcedColumnDef] = columnDefs.map(addSourceTo)
  
  private final def fromCombinator(f: Iterator[CsvRow] => Iterator[CsvRow]): CsvSource = {
    new CsvSource.FromIterator(f(records))
  }
}
  
object CsvSource extends Loggable {
  
  object Defaults {
    val tabDelimited: CSVFormat = CSVFormat.DEFAULT.withDelimiter('\t')
    
    val tabDelimitedWithHeaderCsvFormat: CSVFormat = tabDelimited.withFirstRecordAsHeader
    
    val thisDir: Path = Paths.get(".")
  }
  
  private final class FromIterator(iterator: => Iterator[CsvRow]) extends CsvSource {
    override def records: Iterator[CsvRow] = iterator
  }
  
  final case class FromFile(
      path: Path, 
      csvFormat: CSVFormat = Defaults.tabDelimitedWithHeaderCsvFormat) extends CsvSource {
    
    override def records: Iterator[CsvRow] = toCsvRowIterator(new FileReader(path.toFile), csvFormat)
  }
  
  final case class FromCommand(
      command: String,
      csvFormat: CSVFormat = Defaults.tabDelimitedWithHeaderCsvFormat,
      workDir: Path = Defaults.thisDir) extends CsvSource {
    
    override def records: Iterator[CsvRow] = {
      val bashScriptForCommand = BashScript.fromCommandLineString(command)
      
      val processBuilder = new java.lang.ProcessBuilder(bashScriptForCommand.commandTokens: _*)
      
      val process = processBuilder.start()
            
      toCsvRowIterator(
          new InputStreamReader(process.getInputStream), 
          csvFormat)
    }
  }
  
  def fromCommandLine(
      command: HasCommandLine,
      csvFormat: CSVFormat = Defaults.tabDelimitedWithHeaderCsvFormat,
      workDir: Path = Defaults.thisDir): CsvSource = {
    
    FromCommand(command.commandLineString, csvFormat, workDir)
  }
  
  private def toCsvRowIterator(reader: Reader, csvFormat: CSVFormat): Iterator[CsvRow] = {
    import scala.collection.JavaConverters._
      
    val parser = new CSVParser(reader, csvFormat)
    
    val iterator = parser.iterator.asScala
    
    TakesEndingActionIterator(iterator) {
      Throwables.quietly("Closing CSV parser")(parser.close())
      Throwables.quietly("Closing underlying reader")(reader.close())
    }
  }
}
