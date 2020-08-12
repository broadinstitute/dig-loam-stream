package loamstream.loam.intake

import java.io.FileReader
import java.io.InputStreamReader
import java.io.Reader
import java.nio.file.Path
import java.nio.file.Paths

import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVParser

import loamstream.util.BashScript
import loamstream.util.Loggable
import loamstream.util.TakesEndingActionIterator
import loamstream.util.Throwables
import java.util.zip.GZIPInputStream
import java.io.FileInputStream
import java.io.StringReader

/**
 * @author clint
 * Dec 17, 2019
 */
sealed trait CsvSource {
  def records: Iterator[CsvRow]
  
  def take(n: Int): CsvSource = fromCombinator(_.take(n))
  
  def drop(n: Int): CsvSource = fromCombinator(_.drop(n))
  
  def filter(p: RowPredicate): CsvSource = fromCombinator(_.filter(p))
  
  def filterNot(p: RowPredicate): CsvSource = fromCombinator(_.filterNot(p))
  
  def tee: (CsvSource, CsvSource) = (this, duplicate)
  
  private def duplicate: CsvSource = CsvSource.FromIterator(records.duplicate._2)
  
  private final def addSourceTo(columnDef: UnsourcedColumnDef): SourcedColumnDef = SourcedColumnDef(columnDef, this) 
  
  final def producing(columnDef: UnsourcedColumnDef): SourcedColumnDef = addSourceTo(columnDef)
  
  final def producing(columnDefs: Seq[UnsourcedColumnDef]): Seq[SourcedColumnDef] = columnDefs.map(addSourceTo)
  
  private final def fromCombinator(f: Iterator[CsvRow] => Iterator[CsvRow]): CsvSource = {
    new CsvSource.FromIterator(f(records))
  }
}
  
object CsvSource extends Loggable {
  
  object Formats {
    val spaceDelimited: CSVFormat = CSVFormat.DEFAULT.withDelimiter(' ')
    
    val spaceDelimitedWithHeader: CSVFormat = spaceDelimited.withFirstRecordAsHeader
      
    val tabDelimited: CSVFormat = CSVFormat.DEFAULT.withDelimiter('\t')
    
    val tabDelimitedWithHeader: CSVFormat = tabDelimited.withFirstRecordAsHeader
  }
  
  object Defaults {
    val csvFormat: CSVFormat = Formats.tabDelimitedWithHeader 
    
    val thisDir: Path = Paths.get(".")
  }
  
  private[intake] final class FromIterator(iterator: => Iterator[CsvRow]) extends CsvSource {
    override def records: Iterator[CsvRow] = iterator
  }
  
  private[intake] object FromIterator {
    def apply(iterator: => Iterator[CsvRow]): FromIterator = new FromIterator(iterator)
  }

  def fromFile(
      path: Path, 
      csvFormat: CSVFormat = Defaults.csvFormat): CsvSource = {
    
    fromReader(new FileReader(path.toFile), csvFormat)
  }
  
  def fromGzippedFile(
      path: Path, 
      csvFormat: CSVFormat = Defaults.csvFormat): CsvSource = {
    
    fromReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(path.toFile))), csvFormat)
  }
  
  def fromString(
      csvData: String,
      csvFormat: CSVFormat = Defaults.csvFormat): CsvSource = {
    
    fromReader(new StringReader(csvData), csvFormat)
  }
  
  def fromReader(
      reader: => Reader, 
      csvFormat: CSVFormat = Defaults.csvFormat): CsvSource = {
    
    FromIterator(toCsvRowIterator(reader, csvFormat))
  }

  def fromCommandLine(
      command: String,
      csvFormat: CSVFormat = Defaults.csvFormat,
      workDir: Path = Defaults.thisDir): CsvSource = {
  
    FromIterator {
      val bashScriptForCommand = BashScript.fromCommandLineString(command)
        
      val processBuilder = new java.lang.ProcessBuilder(bashScriptForCommand.commandTokens: _*)
      
      val process = processBuilder.start()
            
      toCsvRowIterator(
          new InputStreamReader(process.getInputStream), 
          csvFormat)
    }
  }
  
  private def toCsvRowIterator(reader: Reader, csvFormat: CSVFormat): Iterator[CsvRow] = {
    import scala.collection.JavaConverters._
      
    val parser = new CSVParser(reader, csvFormat)
    
    val iterator = parser.iterator.asScala.map(CsvRow.CommonsCsvRow(_))
    
    TakesEndingActionIterator(iterator) {
      Throwables.quietly("Closing CSV parser")(parser.close())
      Throwables.quietly("Closing underlying reader")(reader.close())
    }
  }
}
