package loamstream.loam.intake

import loamstream.loam.LoamScriptContext
import loamstream.TestHelpers
import java.nio.file.Path
import loamstream.loam.LoamSyntax
import loamstream.model.Store

/**
 * @author clint
 * Feb 10, 2020
 */
object Helpers {
  def csvRow(columnNamesToValues: (String, String)*)(implicit discriminator: Int = 1): CsvRow = new CsvRow {
    override def toString: String = columnNamesToValues.toString
    
    override def getFieldByName(name: String): String = {
      columnNamesToValues.collectFirst { case (n, v) if name == n => v }.get
    }
  
    override def getFieldByIndex(i: Int): String = columnNamesToValues.unzip._2.apply(i)
    
    override def size: Int = columnNamesToValues.size
    
    override def recordNumber: Long = 1337
  }
  
  def csvRows(columnNames: Seq[String], values: Seq[String]*)(implicit discriminator: Int = 1): Seq[CsvRow] = {
    val rows = values.map(rowValues => columnNames.zip(rowValues))
    
    rows.map(row => csvRow(row: _*))
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
}
