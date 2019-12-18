package loamstream.loam.intake

import java.nio.file.Files
import java.nio.file.Paths
import java.nio.file.Path
import org.apache.commons.csv.CSVParser
import org.apache.commons.csv.CSVFormat
import scala.collection.JavaConverters._
import java.io.FileWriter
import scala.util.Try
import java.io.PrintWriter
import scala.util.control.NonFatal

object Inventory extends App {
  private val file0 = Paths.get("./expressions-col0")
  private val file1 = Paths.get("./expressions-col1")
  private val file2 = Paths.get("./expressions-col2")
  private val file3 = Paths.get("./expressions-col3")
  
  private val writer0 = new PrintWriter(new FileWriter(file0.toFile))
  private val writer1 = new PrintWriter(new FileWriter(file1.toFile))
  private val writer2 = new PrintWriter(new FileWriter(file2.toFile))
  private val writer3 = new PrintWriter(new FileWriter(file3.toFile))
  
  private def quietly(f: => Any): Unit = Try(f)
  
  def close(): Unit = {
    quietly(writer0.close())
    quietly(writer1.close())
    quietly(writer2.close())
    quietly(writer3.close())
  }
  
  final case class ExpressionsByColumn(
      inCol0: Seq[String], 
      inCol1: Seq[String], 
      inCol2: Seq[String], 
      inCol3: Seq[String]) {
    
    def ++(other: ExpressionsByColumn): ExpressionsByColumn = {
      ExpressionsByColumn(
          this.inCol0 ++ other.inCol0,
          this.inCol1 ++ other.inCol1,
          this.inCol2 ++ other.inCol2,
          this.inCol3 ++ other.inCol3)
    }
    
    def write(): Unit = {
      inCol0.distinct.foreach(writer0.println)
      inCol1.distinct.foreach(writer1.println)
      inCol2.distinct.foreach(writer2.println)
      inCol3.distinct.foreach(writer3.println)
    }
  }
  
  object ExpressionsByColumn {
    val empty: ExpressionsByColumn = ExpressionsByColumn(Nil, Nil, Nil, Nil)
  }
  
  def extractExpressions(file: Path): ExpressionsByColumn = {
    val reader = Files.newBufferedReader(file)
            
    val csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withDelimiter('\t'))
    
    val z: ExpressionsByColumn = ExpressionsByColumn.empty
    
    def isExpression(s: String): Boolean = {
      val trimmed = s.trim
      
      trimmed.nonEmpty && trimmed.head == '='
    }
    
    csvParser.asScala.foldLeft(z) { (acc, csvRecord) =>
      val ExpressionsByColumn(acc0, acc1, acc2, acc3) = acc
      
      def get(i: Int): String = {
        try { csvRecord.get(i) }
        catch {
          case NonFatal(e) => throw new Exception(s"Error parsing column $i of '$file'", e)
        }
      }
      
      val col0 = get(0)
      val col1 = get(1)
      val col2 = get(2)
      val col3 = get(3)
      
      val new0 = if(isExpression(col0)) col0 +: acc0 else acc0
      val new1 = if(isExpression(col1)) col1 +: acc1 else acc1
      val new2 = if(isExpression(col2)) col2 +: acc2 else acc2
      val new3 = if(isExpression(col3)) col3 +: acc3 else acc3
      
      ExpressionsByColumn(new0, new1, new2, new3)
    }
  }
  
  //~/workspace/marcins-scripts/VARIANTS/configs
  def files: Seq[Path] = {
    Files.list(Paths.get("/home/clint/workspace/marcins-scripts/VARIANTS/configs")).iterator.asScala.toVector
  }
  
  def parseAllFiles: ExpressionsByColumn = {
    files.map(extractExpressions).foldLeft(ExpressionsByColumn.empty) { _ ++ _ }
  }
  
  try {
    parseAllFiles.write()
  } finally {
    close()
  }
}
