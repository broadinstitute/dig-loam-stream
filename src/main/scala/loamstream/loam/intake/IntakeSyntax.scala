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
import loamstream.loam.intake.aggregator.RowFilters
import loamstream.loam.intake.aggregator.RowTransforms


/**
 * @author clint
 * Dec 20, 2019
 */
object IntakeSyntax extends IntakeSyntax

trait IntakeSyntax extends Interpolators with Metrics with RowFilters with RowTransforms with GraphFunctions {
  type ColumnName = loamstream.loam.intake.ColumnName
  val ColumnName = loamstream.loam.intake.ColumnName
  
  type ColumnExpr[A] = loamstream.loam.intake.ColumnExpr[A]
  val ColumnExpr = loamstream.loam.intake.ColumnExpr
  
  type Variant = loamstream.loam.intake.Variant
  val Variant = loamstream.loam.intake.Variant
  
  type NamedColumnDef[A] = loamstream.loam.intake.NamedColumnDef[A]
  val NamedColumnDef = loamstream.loam.intake.NamedColumnDef
  
  type Source[A] = loamstream.loam.intake.Source[A]
  val Source = loamstream.loam.intake.Source
  
  type FlipDetector = loamstream.loam.intake.flip.FlipDetector
  val FlipDetector = loamstream.loam.intake.flip.FlipDetector
  
  type Row = loamstream.loam.intake.Row
  
  type HeaderRow = loamstream.loam.intake.LiteralRow
  val HeaderRow = loamstream.loam.intake.LiteralRow
  
  //TODO
  type DataRow = loamstream.loam.intake.aggregator.DataRow
  val DataRow = loamstream.loam.intake.aggregator.DataRow
  
  type CsvRow = loamstream.loam.intake.CsvRow
  val CsvRow = loamstream.loam.intake.CsvRow
  
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
  
  private def nativeTool[A](forceLocal: Boolean = false)(body: => A)(implicit scriptContext: LoamScriptContext): Tool = { 
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
  
  private[intake] def headerRowFrom(columnDefs: Seq[NamedColumnDef[_]]): HeaderRow = {
    HeaderRow(columnDefs.sortBy(_.name.index).map(_.name.name))
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
  
  final class ViaTarget(
      dest: Store, 
      rows: Source[CsvRow],
      flipDetector: => FlipDetector) extends Loggable {
    
    def filter(p: RowPredicate): ViaTarget = new ViaTarget(dest, rows.filter(p), flipDetector)
    
    def filter(ps: Iterable[RowPredicate]): ViaTarget = {
      val filteredRows = ps.foldLeft(rows)(_.filter(_))
      
      new ViaTarget(dest, filteredRows, flipDetector)
    }
    
    def via(expr: aggregator.RowExpr): MapFilterAndGoTarget[Unit] = {
      val dataRows = rows.tagFlips(expr.markerDef, flipDetector).map(expr)
      
      val headerRow = headerRowFrom(expr.columnDefs)
      
      val pseudoMetric: Metric[Unit] = Fold.foreach(_ => ()) // TODO
      
      new MapFilterAndGoTarget(dest, headerRow, dataRows, pseudoMetric)
    }
  }
  
  final class MapFilterAndGoTarget[A](
      dest: Store, 
      headerRow: HeaderRow,
      rows: Source[aggregator.DataRow],
      metric: Metric[A]) extends Loggable {
    
    import loamstream.loam.intake.metrics.MetricOps
    
    def copy(
        dest: Store = this.dest, 
        headerRow: HeaderRow = this.headerRow,
        rows: Source[aggregator.DataRow] = this.rows): MapFilterAndGoTarget[A] = {
      
      new MapFilterAndGoTarget(dest, headerRow, rows, metric)
    }
    
    def withMetric[B](m: Metric[B]): MapFilterAndGoTarget[(A, B)] = {
      new MapFilterAndGoTarget[(A, B)](dest, headerRow, rows, metric combine m)
    }
    
    def filter(predicate: aggregator.DataRow => Boolean): MapFilterAndGoTarget[A] = copy(rows = rows.filter(predicate))
    
    def map(transform: aggregator.DataRow => aggregator.DataRow): MapFilterAndGoTarget[A] = {
      copy(rows = rows.map(transform))
    }
    
    //TODO: better name
    def write(forceLocal: Boolean = false)(implicit scriptContext: LoamScriptContext): Tool = {
      require(dest.isPathStore)
      
      val sink: RowSink = RowSink.ToFile(dest.path)
      
      val writeLines: Metric[Unit] = Fold.foreach(sink.accept)
      
      val m: Metric[(A, Unit)] = metric.combine(writeLines)
      
      //TODO: How to wire up inputs (if any)?
      val tool: Tool = nativeTool(forceLocal) {
        TimeUtils.time(s"Producing ${dest.path}", info(_)) {
          CanBeClosed.enclosed(sink) { _ =>
            sink.accept(headerRow)
          
            val (metricResults, _) = m.process(rows)
            
            //TODO: What to do with metricResults?
          }
        }
      }
      
      addToGraph(tool)
      
      tool.out(dest)
    }
  }

  final class AggregatorIntakeConfigFileTarget(dest: Store) {
    def from(configData: aggregator.ConfigData)(implicit scriptContext: LoamScriptContext): Tool = {
      //TODO: How to wire up inputs (if any)?
      val tool: Tool = nativeTool() {
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
