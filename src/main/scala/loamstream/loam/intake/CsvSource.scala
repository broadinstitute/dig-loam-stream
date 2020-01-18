package loamstream.loam.intake

import java.io.FileReader
import java.io.InputStreamReader
import java.io.Reader
import java.nio.file.Path
import java.nio.file.Paths

import org.apache.commons.csv.CSVFormat

import de.siegmar.fastcsv.reader.CsvParser
import de.siegmar.fastcsv.reader.CsvReader
import loamstream.util.BashScript
import loamstream.util.Loggable
import loamstream.util.TakesEndingActionIterator
import loamstream.util.Throwables

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
    object CommonsCsv {
      object Formats {
        val tabDelimited: CSVFormat = CSVFormat.DEFAULT.withDelimiter('\t')
      
        val tabDelimitedWithHeaderCsvFormat: CSVFormat = tabDelimited.withFirstRecordAsHeader
      }
    }
    
    object FastCsv {
      val delimiter: Char = '\t'
      val containsHeader: Boolean = true 
    }
    
    val thisDir: Path = Paths.get(".")
  }
  
  private final class FromIterator(iterator: => Iterator[CsvRow]) extends CsvSource {
    override def records: Iterator[CsvRow] = iterator
  }

  object FastCsv {
    def fromFile(
        file: Path, 
        delimiter: Char = Defaults.FastCsv.delimiter, 
        containsHeader: Boolean = Defaults.FastCsv.containsHeader): CsvSource = {
      
      new FromIterator(toCsvRowIterator(new FileReader(file.toFile), delimiter, containsHeader))
    }
    
    private def toCsvRowIterator(reader: Reader, delimiter: Char, containsHeader: Boolean): Iterator[de.siegmar.fastcsv.reader.CsvRow] = {
        
      val csvReader = new CsvReader
      
      csvReader.setFieldSeparator(delimiter)
      csvReader.setContainsHeader(containsHeader)
      
      val csvParser: CsvParser = csvReader.parse(reader)
      
      import de.siegmar.fastcsv.reader.CsvRow
      
      val iterator: Iterator[CsvRow] = new Iterator[CsvRow] {
        private[this] var currentRow: CsvRow = csvParser.nextRow()
        
        private[this] def read(): Unit = {
          currentRow = csvParser.nextRow()
        }
        
        override def hasNext: Boolean = currentRow != null
        
        override def next(): CsvRow = {
          try { if(currentRow != null) currentRow else throw new Exception("No more CSV rows") } 
          finally { read() }
        }
      }
      
      TakesEndingActionIterator(iterator) {
        Throwables.quietly("Closing CSV parser")(csvParser.close())
        Throwables.quietly("Closing underlying reader")(reader.close())
      }
    }
    
    final case class FromCommand(
        command: String,
        workDir: Path = Defaults.thisDir,
        delimiter: Char = Defaults.FastCsv.delimiter,
        containsHeader: Boolean = Defaults.FastCsv.containsHeader) extends CsvSource {
      
      override def records: Iterator[CsvRow] = {
        val bashScriptForCommand = BashScript.fromCommandLineString(command)
        
        val processBuilder = new java.lang.ProcessBuilder(bashScriptForCommand.commandTokens: _*)
        
        val process = processBuilder.start()
              
        toCsvRowIterator(new InputStreamReader(process.getInputStream), delimiter, containsHeader)
      }
    }
    
    def fromCommandLine(
        command: String,
        workDir: Path = Defaults.thisDir,
        delimiter: Char = Defaults.FastCsv.delimiter,
        containsHeader: Boolean = Defaults.FastCsv.containsHeader): CsvSource = {
    
      FromCommand(command, workDir, delimiter, containsHeader)
    }
  }
}
