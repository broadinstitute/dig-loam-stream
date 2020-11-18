package loamstream.loam.intake

import loamstream.compiler.LoamPredef
import loamstream.loam.GraphFunctions
import loamstream.loam.InvokesLsTool
import loamstream.loam.LoamScriptContext
import loamstream.loam.NativeTool
import loamstream.model.Store
import loamstream.model.Tool
import loamstream.model.execute.DrmSettings
import loamstream.util.Files
import loamstream.util.Loggable
import loamstream.util.TimeUtils
import loamstream.loam.intake.metrics.Metric
import loamstream.util.Fold
import loamstream.util.CanBeClosed
import loamstream.loam.intake.metrics.Metrics
import java.io.Closeable
import loamstream.util.Throwables
import loamstream.util.CompositeException
import loamstream.loam.intake.metrics.BioIndexClient


/**
 * @author clint
 * Dec 20, 2019
 */
object IntakeSyntax extends IntakeSyntax {
  object Defaults {
    val bioIndexClient: BioIndexClient = new BioIndexClient.Default()
  }
}

trait IntakeSyntax extends Interpolators with Metrics with RowFilters with RowTransforms with GraphFunctions {
  type ColumnName = loamstream.loam.intake.ColumnName
  val ColumnName = loamstream.loam.intake.ColumnName
  
  type ColumnExpr[A] = loamstream.loam.intake.ColumnExpr[A]
  val ColumnExpr = loamstream.loam.intake.ColumnExpr
  
  type Variant = loamstream.loam.intake.Variant
  val Variant = loamstream.loam.intake.Variant
  
  type NamedColumnDef[A] = loamstream.loam.intake.NamedColumnDef[A]
  val NamedColumnDef = loamstream.loam.intake.NamedColumnDef
  
  type MarkerColumnDef = loamstream.loam.intake.MarkerColumnDef
  val MarkerColumnDef = loamstream.loam.intake.MarkerColumnDef
  
  type Source[A] = loamstream.loam.intake.Source[A]
  val Source = loamstream.loam.intake.Source
  
  type FlipDetector = loamstream.loam.intake.flip.FlipDetector
  val FlipDetector = loamstream.loam.intake.flip.FlipDetector
  
  type Row = loamstream.loam.intake.Row
  
  type HeaderRow = loamstream.loam.intake.LiteralRow
  val HeaderRow = loamstream.loam.intake.LiteralRow
  
  //TODO
  type DataRow = loamstream.loam.intake.DataRow
  val DataRow = loamstream.loam.intake.DataRow
  
  type CsvRow = loamstream.loam.intake.CsvRow
  val CsvRow = loamstream.loam.intake.CsvRow
  
  type AggregatorMetadata = loamstream.loam.intake.AggregatorMetadata
  val AggregatorMetadata = loamstream.loam.intake.AggregatorMetadata
  
  type AggregatorRowExpr = loamstream.loam.intake.AggregatorRowExpr
  val AggregatorRowExpr = loamstream.loam.intake.AggregatorRowExpr
  
  val AggregatorColumnDefs = loamstream.loam.intake.AggregatorColumnDefs
  
  val AggregatorColumnNames = loamstream.loam.intake.AggregatorColumnNames
  
  object Log {
    def toFile(store: Store, append: Boolean = false): ToFileLogContext = {
      require(store.isPathStore)
      
      new ToFileLogContext(store.path, append)
    }
  }
  
  private def doLocally[A](body: => A)(implicit scriptContext: LoamScriptContext): NativeTool = {
    LoamPredef.local {
      NativeTool {
        body
      }
    }
  }
  
  private def nativeTool[A](
      forceLocal: Boolean = false)(body: => A)(implicit scriptContext: LoamScriptContext): Tool = { 
    if(forceLocal || scriptContext.lsSettings.thisInstanceIsAWorker) {
      doLocally(body) 
    } else {
      scriptContext.settings match {
        case drmSettings: DrmSettings => InvokesLsTool()
        case settings => {
          sys.error(
              s"Intake jobs can only run locally with --worker or on a DRM system, but settings were $settings")
        }
      }
    }
  }
  
  def requireKnownPhenotype(
      phenotype: Phenotype, 
      bioIndexClient: BioIndexClient = IntakeSyntax.Defaults.bioIndexClient): Unit = {
    
    require(bioIndexClient.isKnown(phenotype), s"Phenotype '${phenotype.name}' was not known to the BioIndex")
  }
 
  def requireKnownDataset(
      dataset: Dataset, 
      bioIndexClient: BioIndexClient = IntakeSyntax.Defaults.bioIndexClient): Unit = {
    
    require(bioIndexClient.isKnown(dataset), s"Dataset '${dataset.name}' was not known to the BioIndex")
  }
  
  def fixPhenotypeCase(
      phenotype: Phenotype, 
      bioIndexClient: BioIndexClient = IntakeSyntax.Defaults.bioIndexClient): Phenotype = {
    
    bioIndexClient.findClosestMatch(phenotype).getOrElse {
      val msg = s"Couldn't find any case-insensitive matches for phenotype '${phenotype.name}' in the BioIndex"
      
      throw new Exception(msg)
    }
  }
  
  def fixDatasetCase(
      dataset: Dataset, 
      bioIndexClient: BioIndexClient = IntakeSyntax.Defaults.bioIndexClient): Dataset = {
    
    bioIndexClient.findClosestMatch(dataset).getOrElse {
      throw new Exception(s"Couldn't find any case-insensitive matches for dataset '${dataset.name}' in the BioIndex")
    }
  }
  
  private[intake] def headerRowFrom(columnDefs: Seq[ColumnName]): HeaderRow = {
    HeaderRow(columnDefs.map(_.name))
  }
  
  implicit final class ColumnNameOps(val s: String) {
    def asColumnName: ColumnName = ColumnName(s)
  }
  
  final class TransformationTarget(dest: Store) {
    def from(source: Source[CsvRow]): UsingTarget = new UsingTarget(dest, source)
  }
  
  final class UsingTarget(dest: Store, rows: Source[CsvRow]) extends Loggable {
    def using(flipDetector: => FlipDetector): ViaTarget = new ViaTarget(dest, rows, flipDetector)
  }
  
  private def asCloseable[A](a: AnyRef): Seq[Closeable] = Option(a).collect { case c: Closeable => c }.toSeq
  
  final class ViaTarget(
      dest: Store, 
      rows: Source[CsvRow],
      flipDetector: => FlipDetector,
      toBeClosed: Seq[Closeable] = Nil) extends Loggable {
    
    def copy(
      dest: Store = this.dest, 
      rows: Source[CsvRow] = this.rows,
      flipDetector: => FlipDetector = this.flipDetector,
      toBeClosed: Seq[Closeable] = this.toBeClosed): ViaTarget = new ViaTarget(dest, rows, flipDetector, toBeClosed) 
    
    private def toFilterTransform(p: RowPredicate): CsvRow => CsvRow = { row =>
      if(p(row)) row else CsvRow.SkippedRaw(derivedFrom = row)
    }
    
    def filter(p: RowPredicate): ViaTarget = {
      copy(rows = rows.map(toFilterTransform(p)), toBeClosed = asCloseable(p) ++ toBeClosed)
    }
    
    def filter(ps: Iterable[RowPredicate]): ViaTarget = {
      val filterTransforms = ps.map(toFilterTransform)
      
      val transformedRows = filterTransforms.foldLeft(rows)(_.map(_))
      
      val psToBeClosed = ps.collect { case c: Closeable => c }.toSeq
      
      copy(rows = transformedRows, toBeClosed = psToBeClosed ++ toBeClosed)
    }
    
    def via(expr: AggregatorRowExpr): MapFilterAndWriteTarget[Unit] = {
      val dataRows = rows.tagFlips(expr.markerDef, flipDetector).map(expr)
      
      val headerRow = headerRowFrom(expr.columnNames)
      
      val pseudoMetric: Metric[Unit] = Fold.foreach(_ => ()) // TODO :(
      
      new MapFilterAndWriteTarget(dest, headerRow, dataRows, pseudoMetric, toBeClosed)
    }
  }
  
  final class MapFilterAndWriteTarget[A](
      dest: Store, 
      headerRow: HeaderRow,
      rows: Source[CsvRow.Parsed],
      metric: Metric[A],
      toBeClosed: Seq[Closeable]) extends Loggable {
    
    import loamstream.loam.intake.metrics.MetricOps
    
    def copy(
        dest: Store = this.dest, 
        headerRow: HeaderRow = this.headerRow,
        rows: Source[CsvRow.Parsed] = this.rows,
        metric: Metric[A] = this.metric,
        toBeClosed: Seq[Closeable] = this.toBeClosed): MapFilterAndWriteTarget[A] = {
      
      new MapFilterAndWriteTarget(dest, headerRow, rows, metric, toBeClosed)
    }
    
    def withMetric[B](m: Metric[B]): MapFilterAndWriteTarget[(A, B)] = {
      val newMetric = metric combine m
      
      new MapFilterAndWriteTarget[(A, B)](dest, headerRow, rows, newMetric, toBeClosed)
    }
    
    def filter(predicate: Predicate[DataRow]): MapFilterAndWriteTarget[A] = {
      def filterTransform(row: CsvRow.Parsed): CsvRow.Parsed = row match {
        case t @ CsvRow.Transformed(_, dataRow) => if(predicate(dataRow)) t else t.skip
        case r: CsvRow.Skipped  => r
      }
      
      copy(rows = rows.map(filterTransform), toBeClosed = asCloseable(predicate) ++ toBeClosed)
    }
    
    def map(transform: Transform[DataRow]): MapFilterAndWriteTarget[A] = {
      def dataRowTransform(row: CsvRow.Parsed): CsvRow.Parsed = row.transform(transform)
      
      copy(rows = rows.map(dataRowTransform), toBeClosed = asCloseable(transform) ++ toBeClosed)
    }
    
    //TODO: better name
    def write(forceLocal: Boolean = false)(implicit scriptContext: LoamScriptContext): Tool = {
      require(dest.isPathStore)
      
      val sink: RowSink = RowSink.ToFile(dest.path)
      
      val writeLines: Metric[Unit] = Fold.foreach(_.dataRowOpt.foreach(sink.accept))
      
      val m: Metric[(A, Unit)] = metric.combine(writeLines)
      
      //TODO: How to wire up inputs (if any)?
      val tool: Tool = nativeTool(forceLocal) {
        TimeUtils.time(s"Producing ${dest.path}", info(_)) {
          CanBeClosed.enclosed(sink) { _ =>
            try {
              sink.accept(headerRow)
              
              val (metricResults, _) = m.process(rows)
            
              //TODO: What to do with metricResults?
            } finally {
              closeEverything()
            }
          }
        }
      }
      
      addToGraph(tool)
      
      tool.out(dest)
    }
    
    private def closeEverything(): Unit = {
      val doCloseBlocks: Seq[() => Any] = toBeClosed.map(closeable => () => closeable.close())
      
      val exceptions = Throwables.collectFailures(doCloseBlocks: _*)
      
      if(exceptions.nonEmpty) {
        throw new CompositeException(exceptions)
      }
    }
  }

  final class AggregatorIntakeConfigFileTarget(dest: Store) {
    def from(
        configData: AggregatorConfigData, 
        forceLocal: Boolean = false)(implicit scriptContext: LoamScriptContext): Tool = {
      
      //TODO: How to wire up inputs (if any)?
      val tool: Tool = nativeTool(forceLocal) {
        Files.writeTo(dest.path)(configData.asConfigFileContents)
      }
      
      addToGraph(tool)
      
      tool.out(dest)
    }
  }
  
  private def requireFsPath(s: Store): Unit = {
    require(s.isPathStore, s"Only writing to a destination on the FS is supported, but got ${s}")
  }
  
  def produceCsv(dest: Store): TransformationTarget = {
    requireFsPath(dest)
    
    new TransformationTarget(dest)
  }
  
  def produceAggregatorIntakeConfigFile(dest: Store) = {
    requireFsPath(dest)
    
    new AggregatorIntakeConfigFileTarget(dest)
  }
}
