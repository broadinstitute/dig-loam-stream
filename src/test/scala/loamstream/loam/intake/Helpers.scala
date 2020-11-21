package loamstream.loam.intake

import loamstream.loam.LoamScriptContext
import loamstream.TestHelpers
import java.nio.file.Path
import loamstream.loam.LoamSyntax
import loamstream.model.Store
import loamstream.loam.intake.flip.FlipDetector
import loamstream.loam.intake.flip.Disposition

/**
 * @author clint
 * Feb 10, 2020
 */
object Helpers {
  private final case class SkippableMockCsvRow(
      isSkipped: Boolean, 
      columnNamesToValues: (String, String)*) extends CsvRow {
    
    override def toString: String = columnNamesToValues.toString
    
    override def getFieldByName(name: String): String = {
      columnNamesToValues.collectFirst { case (n, v) if name == n => v }.get
    }
  
    override def getFieldByIndex(i: Int): String = columnNamesToValues.unzip._2.apply(i)
    
    override def size: Int = columnNamesToValues.size
    
    override def recordNumber: Long = 1337
    
    override def skip: CsvRow = SkippableMockCsvRow(isSkipped = true, columnNamesToValues: _*)
  }
  
  def csvRow(columnNamesToValues: (String, String)*): CsvRow = SkippableMockCsvRow(false, columnNamesToValues: _*)
  
  def csvRows(columnNames: Seq[String], values: Seq[String]*): Seq[CsvRow] = {
    val rows = values.map(rowValues => columnNames.zip(rowValues))
    
    rows.map(row => csvRow(row: _*))
  }
  
  def sourceProducing(columnNames: Seq[String], values: Seq[String]*): Source[CsvRow] = {
    Source.fromIterable(csvRows(columnNames, values: _*))
  }
  
  def withLogStore[A](f: Store => A): A = {
    TestHelpers.withWorkDir(getClass.getSimpleName) { workDir =>
      TestHelpers.withScriptContext { implicit context =>
        f(logStoreIn(workDir))
      }
    }
  }
  
  def logStoreIn(workDir: Path, name: String = "blarg.log")(implicit ctx: LoamScriptContext): Store = {
    val file = workDir.resolve(name)
    
    import LoamSyntax._
    
    store(file)
  }
  
  def linesFrom(path: Path): Seq[String] = {
    import scala.collection.JavaConverters._
    
    java.nio.file.Files.readAllLines(path).asScala.toSeq
  }
  
  object Implicits {
    final implicit class LogFileOps(val lines: Seq[String]) extends AnyVal {
      def containsOnce(s: String): Boolean = lines.count(_.contains(s)) == 1
    }
  }
  
  object FlipDetectors {
    object NoFlipsEver extends FlipDetector {
      override def isFlipped(variantId: Variant): Disposition = Disposition.NotFlippedSameStrand
    }
  }
}
