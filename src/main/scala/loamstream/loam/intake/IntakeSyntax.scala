package loamstream.loam.intake

import loamstream.loam.NativeTool
import loamstream.loam.LoamScriptContext
import loamstream.util.Maps
import org.apache.commons.csv.CSVRecord
import loamstream.model.Store
import loamstream.util.Files
import loamstream.model.Tool


/**
 * @author clint
 * Dec 20, 2019
 */
object IntakeSyntax extends IntakeSyntax

trait IntakeSyntax extends Interpolators {
  implicit final class ColumnNameOps(val s: String) {
    def asColumnName: ColumnName = ColumnName(s)
  }
  
  final class TransformationTarget(dest: Store) {
    def from(columnDefs: SourcedColumnDef*)(implicit scriptContext: LoamScriptContext): NativeTool = {
      //TODO: How to wire up inputs (if any)?
      val tool = NativeTool {
        val (headerRow, dataRows) = process(columnDefs)
        
        val csvFormat = CsvSource.Defaults.tabDelimitedWithHeaderCsvFormat
        
        val renderer = Renderer(csvFormat)
        
        val rowsToWrite: Iterator[Row] = Iterator(headerRow) ++ dataRows
        
        
        
        Files.writeLinesTo(dest.path)(rowsToWrite.map(renderer.render))
      }.out(dest)
      
      addToGraph(tool)
      
      tool
    }
  }
  
  final class SchemaFileTarget(dest: Store) {
    def from(columnDefs: ColumnDef*)(implicit scriptContext: LoamScriptContext): NativeTool = {
      val headerRow = headerRowFrom(columnDefs)
      
      val csvFormat = CsvSource.Defaults.tabDelimitedWithHeaderCsvFormat
      
      //TODO: How to wire up inputs (if any)?
      val tool = NativeTool {
        Files.writeTo(dest.path)(headerRow.toSchemaFile(csvFormat))
      }.out(dest)
      
      addToGraph(tool)
      
      tool
    }
  }
  
  /** BEWARE: This method has the side-effect of modifying the graph within scriptContext */
  private def addToGraph(tool: Tool)(implicit scriptContext: LoamScriptContext): Unit = {
    scriptContext.projectContext.updateGraph { graph =>
      graph.withTool(tool, scriptContext)
    }
  }
  
  private def requireFsPath(s: Store): Unit = {
    require(s.isPathStore, s"Only writing to a destination on the FS is supported, but got ${s}")
  }
  
  def produceSchemaFile(dest: Store): SchemaFileTarget = {
    requireFsPath(dest)
    
    new SchemaFileTarget(dest)
  }
  
  def produceCsv(dest: Store): TransformationTarget = {
    requireFsPath(dest)
    
    new TransformationTarget(dest)
  }
  
  private[intake] def fuse(columnDefs: Seq[ColumnDef]): ParseFn = {
    row => {
      val dataRowValues: Map[ColumnDef, TypedData] = Map.empty ++ columnDefs.map { columnDef =>
        val typedColumnValue = columnDef.getTypedValueFromSource(row)
        
        columnDef -> typedColumnValue
      }
    
      DataRow(dataRowValues)
    }
  }
  
  private def headerRowFrom(columnDefs: Seq[ColumnDef]): HeaderRow = {
    HeaderRow(columnDefs.sortBy(_.index).map(cd => (cd.name.name, cd.getValueFromSource.dataType)))
  }

  def process(columnDefs: Seq[SourcedColumnDef]): (HeaderRow, Iterator[DataRow]) = {
    val bySource = columnDefs.groupBy(_.source)
    
    import Maps.Implicits._
    
    val parsingFunctionsBySource = bySource.strictMapValues(fuse).toSeq
    
    val header = headerRowFrom(columnDefs)
    
    val isToFns = parsingFunctionsBySource.map { case (src, parseFn) => 
      (src.records, parseFn) 
    }
    
    val rows = new Iterator[DataRow] {
      override def hasNext: Boolean = isToFns.forall { case (records, _) => records.hasNext }
      override def next(): DataRow = {
        def parseNext(t: (Iterator[CSVRecord], CSVRecord => DataRow)): DataRow = {
          val (records, parseFn) = t
          
          val record = records.next()
          
          parseFn(record)
        }
        
        isToFns.map(parseNext).foldLeft(DataRow.empty)(_ ++ _)
      }
    }
    
    (header, rows)
  }
}
