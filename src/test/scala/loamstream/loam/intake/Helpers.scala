package loamstream.loam.intake

import loamstream.loam.LoamScriptContext
import loamstream.TestHelpers
import java.nio.file.Path
import loamstream.loam.LoamSyntax
import loamstream.model.Store
import loamstream.loam.intake.flip.FlipDetector
import loamstream.loam.intake.flip.Disposition
import loamstream.loam.intake.metrics.BioIndexClient

/**
 * @author clint
 * Feb 10, 2020
 */
object Helpers {
  private final case class SkippableMockCsvRow(
      isSkipped: Boolean, 
      columnNamesToValues: (String, String)*) extends DataRow {
    
    override def headers: Seq[String] = columnNamesToValues.unzip._1
    
    override def toString: String = columnNamesToValues.toString
    
    override def hasField(name: String): Boolean = columnNamesToValues.exists { case (k, _) => k == name }
    
    override def getFieldByName(name: String): String = {
      val opt = columnNamesToValues.collectFirst { case (n, v) if name == n => v }
      
      opt match {
        case Some(result) => result
        case None => throw new CsvProcessingException(s"Couldn't find column '${name}' in ${this}", this, null, null)
      }
    }
  
    override def getFieldByIndex(i: Int): String = columnNamesToValues.unzip._2.apply(i)
    
    override def size: Int = columnNamesToValues.size
    
    override def recordNumber: Long = 1337
    
    override def skip: DataRow = SkippableMockCsvRow(isSkipped = true, columnNamesToValues: _*)
  }
  
  def csvRow(columnNamesToValues: (String, String)*): DataRow = SkippableMockCsvRow(false, columnNamesToValues: _*)
  
  def csvRows(columnNames: Seq[String], values: Seq[String]*): Seq[DataRow] = {
    val rows = values.map(rowValues => columnNames.zip(rowValues))
    
    rows.map(row => csvRow(row: _*))
  }
  
  def sourceProducing(columnNames: Seq[String], values: Seq[String]*): Source[DataRow] = {
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
  
  object BioIndexClients {
    final case class Mock(
        knownVariants: Set[Variant] = Set.empty,
        knownDatasets: Set[Dataset] = Set.empty,
        knownPhenotypes: Set[Phenotype] = Set.empty) extends BioIndexClient {

      override def isKnown(variant: Variant): Boolean = knownVariants.contains(variant) 
      
      override def isKnown(dataset: Dataset): Boolean = knownDatasets.contains(dataset)
      
      override def isKnown(phenotype: Phenotype): Boolean = knownPhenotypes.contains(phenotype)
      
      override def findClosestMatch(dataset: Dataset): Option[Dataset] = {
        knownDatasets.find(_.name.toUpperCase == dataset.name.toUpperCase)
      }
      
      override def findClosestMatch(phenotype: Phenotype): Option[Phenotype] = {
        knownPhenotypes.find(_.name.toUpperCase == phenotype.name.toUpperCase)
      }
    }
  }
  
  object RowSinks {
    def noop[R]: RowSink[R] = new RowSink[R] {
      override def accept(row: R): Unit = ()
      
      override def close(): Unit = ()
    }
  }
}
