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
import loamstream.loam.intake.flip.FlipDetector

/**
 * @author clint
 * Dec 17, 2019
 */
sealed trait Source[R] {
  def records: Iterator[R]
  
  def take(n: Int): Source[R] = fromCombinator(_.take(n))
  
  def drop(n: Int): Source[R] = fromCombinator(_.drop(n))
  
  def filter(p: R => Boolean): Source[R] = fromCombinator(_.filter(p))
  
  def filterNot(p: R => Boolean): Source[R] = fromCombinator(_.filterNot(p))
  
  def tee: (Source[R], Source[R]) = (this, duplicate)
  
  private def duplicate: Source[R] = Source.FromIterator(records.duplicate._2)
  
  private final def fromCombinator[S](f: Iterator[R] => Iterator[S]): Source[S] = {
    new Source.FromIterator(f(records))
  }
  
  def tagFlips(
      markerDef: ColumnDef[String], 
      flipDetector: => FlipDetector)(implicit ev: R <:< CsvRow): Source[CsvRow.WithFlipTag] = {
    
    lazy val actualFlipDetector = flipDetector
    
    this.map(ev).map { row =>
      val marker = markerDef.expr.apply(row)
    
      val isFlipped = actualFlipDetector.isFlipped(marker)
    
      CsvRow.TaggedCsvRow(row, marker, isFlipped)
    }
  }
  
  def map[S](f: R => S): Source[S] = fromCombinator(_.map(f))
  
  def flatMap[S](f: R => Source[S]): Source[S] = fromCombinator(_.flatMap(f(_).records))
}
  
object Source extends Loggable {
  
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
  
  private[intake] final class FromIterator[R](iterator: => Iterator[R]) extends Source[R] {
    override def records: Iterator[R] = iterator
  }
  
  private[intake] object FromIterator {
    def apply[R](iterator: => Iterator[R]): FromIterator[R] = new FromIterator(iterator)
  }

  def fromFile(
      path: Path, 
      csvFormat: CSVFormat = Defaults.csvFormat): Source[CsvRow] = {
    
    fromReader(new FileReader(path.toFile), csvFormat)
  }
  
  def fromGzippedFile(
      path: Path, 
      csvFormat: CSVFormat = Defaults.csvFormat): Source[CsvRow] = {
    
    fromReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(path.toFile))), csvFormat)
  }
  
  def fromString(
      csvData: String,
      csvFormat: CSVFormat = Defaults.csvFormat): Source[CsvRow] = {
    
    fromReader(new StringReader(csvData), csvFormat)
  }
  
  def fromReader(
      reader: => Reader, 
      csvFormat: CSVFormat = Defaults.csvFormat): Source[CsvRow] = {
    
    FromIterator(toCsvRowIterator(reader, csvFormat))
  }

  def fromCommandLine(
      command: String,
      csvFormat: CSVFormat = Defaults.csvFormat,
      workDir: Path = Defaults.thisDir): Source[CsvRow] = {
  
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