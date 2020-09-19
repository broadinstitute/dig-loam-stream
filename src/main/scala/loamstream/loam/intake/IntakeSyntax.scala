package loamstream.loam.intake

import loamstream.loam.LoamScriptContext
import loamstream.loam.NativeTool
import loamstream.model.Store
import loamstream.model.Tool
import loamstream.util.Files
import loamstream.util.Hashes
import loamstream.util.TimeUtils
import loamstream.util.Loggable
import loamstream.loam.GraphFunctions
import loamstream.model.execute.LocalSettings
import loamstream.model.execute.DrmSettings
import loamstream.loam.InvokesLsTool
import loamstream.compiler.LoamPredef


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
  
  type ColumnDef = loamstream.loam.intake.ColumnDef
  val ColumnDef = loamstream.loam.intake.ColumnDef
  
  type UnsourcedColumnDef = loamstream.loam.intake.UnsourcedColumnDef
  val UnsourcedColumnDef = loamstream.loam.intake.UnsourcedColumnDef
  
  type SourcedColumnDef = loamstream.loam.intake.SourcedColumnDef
  val SourcedColumnDef = loamstream.loam.intake.SourcedColumnDef
  
  type RowDef = loamstream.loam.intake.RowDef
  val RowDef = loamstream.loam.intake.RowDef
  
  type UnsourcedRowDef = loamstream.loam.intake.UnsourcedRowDef
  val UnsourcedRowDef = loamstream.loam.intake.UnsourcedRowDef
  
  type CsvSource = loamstream.loam.intake.CsvSource
  val CsvSource = loamstream.loam.intake.CsvSource
  
  type FlipDetector = loamstream.loam.intake.flip.FlipDetector
  val FlipDetector = loamstream.loam.intake.flip.FlipDetector
  
  type Row = loamstream.loam.intake.Row
  
  type HeaderRow = loamstream.loam.intake.HeaderRow
  val HeaderRow = loamstream.loam.intake.HeaderRow
  
  type DataRow = loamstream.loam.intake.DataRow
  val DataRow = loamstream.loam.intake.DataRow
  
  type CsvRow = loamstream.loam.intake.CsvRow
  val CsvRow = loamstream.loam.intake.CsvRow
  
  private def nativeTool[A](body: => A)(implicit scriptContext: LoamScriptContext): NativeTool = {
    require(
        scriptContext.lsSettings.thisInstanceIsAWorker,
        s"Only running native jobs in --worker mode is supported")
        
    LoamPredef.local {
      NativeTool {
        body
      }
    }
  }
  
  implicit final class ColumnNameOps(val s: String) {
    def asColumnName: ColumnName = ColumnName(s)
  }
  
  final class TransformationTarget(dest: Store) {
    def from(rowDef: RowDef): ProcessTarget = from(rowDef.varIdDef, rowDef.otherColumns: _*)
    
    def from(varIdColumnDef: SourcedColumnDef, otherColumnDefs: SourcedColumnDef*): ProcessTarget = {
      new ProcessTarget(dest, varIdColumnDef, otherColumnDefs: _*)
    }
  }    
    
  final class ProcessTarget(
      dest: Store, 
      varIdColumnDef: SourcedColumnDef, 
      otherColumnDefs: SourcedColumnDef*) extends Loggable {
    
    def using(flipDetector: => FlipDetector)(implicit scriptContext: LoamScriptContext): Tool = {
      //TODO: How to wire up inputs (if any)?
      def doIt: NativeTool = nativeTool {  
        TimeUtils.time(s"Producing ${dest.path}", info(_)) {
          val (headerRow, dataRows) = process(flipDetector)(RowDef(varIdColumnDef, otherColumnDefs))
          
          //TODO
          val csvFormat = CsvSource.Defaults.csvFormat
          
          val renderer = Renderer.CommonsCsv(csvFormat)
          
          val rowsToWrite: Iterator[Row] = Iterator(headerRow) ++ dataRows
          
          Files.writeLinesTo(dest.path)(rowsToWrite.map(renderer.render))
        }
      }
      
      val tool: Tool = {
        if(scriptContext.lsSettings.thisInstanceIsAWorker) { doIt }
        else {
          scriptContext.settings match {
            case drmSettings: DrmSettings => InvokesLsTool()
            case settings => {
              sys.error(
                  s"Intake jobs can only run locally with --worker or on a DRM system, but settings were $settings")
            }
          }
        }
      }
      
      addToGraph(tool)
      
      tool.out(dest)
    }
  }
  
  final class HashFileTarget(dest: Store) {
    def from(fileToBeHashed: Store)(implicit scriptContext: LoamScriptContext): Tool = {
      //TODO: How to wire up inputs (if any)?
      val tool = NativeTool {
        val hash = Hashes.md5(fileToBeHashed.path).valueAsBase64String 
        
        Files.writeTo(dest.path)(hash)
      }
      
      addToGraph(tool)
      
      tool.out(dest)
    }
  }
  
  final class AggregatorIntakeConfigFileTarget(dest: Store) {
    def from(configData: aggregator.ConfigData)(implicit scriptContext: LoamScriptContext): Tool = {
      def doIt: NativeTool = nativeTool {
        Files.writeTo(dest.path)(configData.asConfigFileContents)
      }
      
      //TODO: How to wire up inputs (if any)?
      val tool: Tool = {
        if(scriptContext.lsSettings.thisInstanceIsAWorker) { doIt }
        else {
          scriptContext.settings match {
            case drmSettings: DrmSettings => InvokesLsTool("fake-stub-tag-name")
            case settings => sys.error(
              s"Intake jobs can only run locally with --worker or on a DRM system, but settings were $settings")
          }
        }
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
  
  def produceMd5Hash(dest: Store): HashFileTarget = {
    requireFsPath(dest)
    
    new HashFileTarget(dest)
  }
  
  def produceAggregatorIntakeConfigFile(dest: Store) = {
    requireFsPath(dest)
    
    new AggregatorIntakeConfigFileTarget(dest)
  }
}
