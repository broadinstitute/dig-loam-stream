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


/**
 * @author clint
 * Dec 20, 2019
 */
object IntakeSyntax extends IntakeSyntax

trait IntakeSyntax extends Interpolators with CsvTransformations with GraphFunctions {
  type ColumnName = loamstream.loam.intake.ColumnName
  val ColumnName = loamstream.loam.intake.ColumnName
  
  type ColumnExpr[A] = loamstream.loam.intake.ColumnExpr[A]
  val ColumnExpr = loamstream.loam.intake.ColumnExpr
  
  type Variant = loamstream.loam.intake.Variant
  val Variant = loamstream.loam.intake.Variant
  
  type NamedColumnDef[A] = loamstream.loam.intake.NamedColumnDef[A]
  val NamedColumnDef = loamstream.loam.intake.NamedColumnDef
  
  type RowDef = loamstream.loam.intake.RowDef
  val RowDef = loamstream.loam.intake.RowDef
  
  @deprecated
  type CsvSource[A] = loamstream.loam.intake.RowSource[A]
  @deprecated
  val CsvSource = loamstream.loam.intake.RowSource
  
  type RowSource[A] = loamstream.loam.intake.RowSource[A]
  val RowSource = loamstream.loam.intake.RowSource
  
  type FlipDetector = loamstream.loam.intake.flip.FlipDetector
  val FlipDetector = loamstream.loam.intake.flip.FlipDetector
  
  type Row = loamstream.loam.intake.Row
  
  type HeaderRow = loamstream.loam.intake.HeaderRow
  val HeaderRow = loamstream.loam.intake.HeaderRow
  
  type DataRow = loamstream.loam.intake.DataRow
  val DataRow = loamstream.loam.intake.DataRow
  
  type CsvRow = loamstream.loam.intake.CsvRow
  val CsvRow = loamstream.loam.intake.CsvRow
  
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
  
  implicit final class ColumnNameOps(val s: String) {
    def asColumnName: ColumnName = ColumnName(s)
  }
  
  final class TransformationTarget(dest: Store) {
    def from(source: RowSource[CsvRow]): UsingTarget = new UsingTarget(dest, source)
    
    def from(rowDef: RowDef): ProcessTarget = from(rowDef.varIdDef, rowDef.otherColumns: _*)
    
    def from(varIdColumnDef: NamedColumnDef[String], otherColumnDefs: NamedColumnDef[_]*): ProcessTarget = {
      new ProcessTarget(dest, varIdColumnDef, otherColumnDefs: _*)
    }
  }
  
  final class UsingTarget(
      dest: Store, 
      rows: RowSource[CsvRow]) extends Loggable {
    
    def using(flipDetector: => FlipDetector): ViaTarget = new ViaTarget(dest, rows, flipDetector)
  }
  
  final class ViaTarget(
      dest: Store, 
      rows: RowSource[CsvRow],
      flipDetector: => FlipDetector) extends Loggable {
    
    def via(expr: aggregator.AggregatorRowExpr): ViaTarget2 = {
      val dataRows = rows.tagFlips(expr.markerDef, flipDetector).map(expr)
      
      val headerRow = headerRowFrom(expr.columnDefs)
      
      new ViaTarget2(dest, headerRow, dataRows)
    }
  }
  
  final case class ViaTarget2(
      dest: Store, 
      headerRow: HeaderRow,
      rows: RowSource[aggregator.DataRow]) extends Loggable {
    
    def filter(predicate: aggregator.DataRow => Boolean): ViaTarget2 = copy(rows = rows.filter(predicate))
    
    def map(transform: aggregator.DataRow => aggregator.DataRow): ViaTarget2 = copy(rows = rows.map(transform))
    
    //TODO: better name
    def go(forceLocal: Boolean = false)(implicit scriptContext: LoamScriptContext): Tool = {
      //TODO: How to wire up inputs (if any)?
      val tool: Tool = nativeTool(forceLocal) {
        TimeUtils.time(s"Producing ${dest.path}", info(_)) {
          //TODO
          val csvFormat = RowSource.Defaults.csvFormat
          
          val renderer = Renderer.CommonsCsv(csvFormat)
          
          val rowsToWrite: Iterator[Row] = Iterator(headerRow) ++ rows.records.map(_.toIntakeRow)
          
          Files.writeLinesTo(dest.path)(rowsToWrite.map(renderer.render))
        }
      }
      
      addToGraph(tool)
      
      tool.out(dest)
    }
  }

  final class ProcessTarget(
      dest: Store, 
      varIdColumnDef: NamedColumnDef[String], 
      otherColumnDefs: NamedColumnDef[_]*) extends Loggable {
    
    def using(flipDetector: => FlipDetector)(implicit scriptContext: LoamScriptContext): Tool = {
      //TODO: How to wire up inputs (if any)?
      val tool: Tool = nativeTool() {
        TimeUtils.time(s"Producing ${dest.path}", info(_)) {
          val (headerRow, dataRows) = process(flipDetector)(RowDef(varIdColumnDef, otherColumnDefs))
          
          //TODO
          val csvFormat = CsvSource.Defaults.csvFormat
          
          val renderer = Renderer.CommonsCsv(csvFormat)
          
          val rowsToWrite: Iterator[Row] = Iterator(headerRow) ++ dataRows
          
          Files.writeLinesTo(dest.path)(rowsToWrite.map(renderer.render))
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
