package loamstream.loam.intake

import loamstream.loam.NativeTool
import loamstream.loam.LoamScriptContext
import loamstream.util.Maps
import loamstream.model.Store
import loamstream.util.Files
import loamstream.model.Tool
import loamstream.model.PathStore
import loamstream.util.Hashes


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
    def from(varIdColumnDef: SourcedColumnDef, otherColumnDefs: SourcedColumnDef*): ProcessTarget = {
      new ProcessTarget(dest, varIdColumnDef, otherColumnDefs: _*)
    }
  }    
    
  final class ProcessTarget(dest: Store, varIdColumnDef: SourcedColumnDef, otherColumnDefs: SourcedColumnDef*) {
    def using(flipDetector: FlipDetector)(implicit scriptContext: LoamScriptContext): NativeTool = {
      //TODO: How to wire up inputs (if any)?
      val tool = NativeTool {
        val (headerRow, dataRows) = process(???)(RowDef(varIdColumnDef, otherColumnDefs))
        
        val csvFormat = CsvSource.Defaults.tabDelimitedWithHeaderCsvFormat
        
        val renderer = CommonsCsvRenderer(csvFormat)
        
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
  
  final class ListFilesTarget(dataListFile: Store, schemaListFile: Store) {
    def from(dataFile: Store, schemaFile: Store)(implicit scriptContext: LoamScriptContext): NativeTool = {
      //TODO: How to wire up inputs (if any)?
      val tool = NativeTool {
        Files.writeLinesTo(dataListFile.path)(Iterator(dataFile.path.toString))
        Files.writeLinesTo(schemaListFile.path)(Iterator(schemaFile.path.toString))
      }.out(dataListFile, schemaListFile)
      
      addToGraph(tool)
      
      tool
    }
  }
  
  final class HashFileTarget(dest: Store) {
    def from(fileToBeHashed: Store)(implicit scriptContext: LoamScriptContext): NativeTool = {
      //TODO: How to wire up inputs (if any)?
      val tool = NativeTool {
        val hash = Hashes.md5(fileToBeHashed.path).valueAsBase64String 
        
        Files.writeTo(dest.path)(hash)
      }
      
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
  
  def produceListFiles(dataListFile: Store, schemaListFile: Store): ListFilesTarget = {
    requireFsPath(dataListFile)
    requireFsPath(schemaListFile)
    
    new ListFilesTarget(dataListFile, schemaListFile)
  }
  
  def produceCsv(dest: Store): TransformationTarget = {
    requireFsPath(dest)
    
    new TransformationTarget(dest)
  }
  
  def produceMd5Hash(dest: Store): HashFileTarget = {
    requireFsPath(dest)
    
    new HashFileTarget(dest)
  }
  
  private[intake] def fuse(flipDetector: FlipDetector)(columnDefs: Seq[ColumnDef]): ParseFn = { (varIdValue, row) =>
    val dataRowValues: Map[ColumnDef, TypedData] = {
      Map.empty ++ columnDefs.map { columnDef =>
        val columnValueFn: RowParser[TypedData] = {
          if(flipDetector.isFlipped(varIdValue)) { columnDef.getTypedValueFromSourceWhenFlipNeeded }
          else { columnDef.getTypedValueFromSource } 
        }
        
        val typedColumnValue = columnValueFn(row)
        
        columnDef -> typedColumnValue
      }
    }
      
    DataRow(dataRowValues)
  }
  
  private def headerRowFrom(columnDefs: Seq[ColumnDef]): HeaderRow = {
    HeaderRow(columnDefs.sortBy(_.index).map(cd => (cd.name.name, cd.getValueFromSource.dataType)))
  }

  def process(flipDetector: FlipDetector)(rowDef: RowDef): (HeaderRow, Iterator[DataRow]) = {
    val varIdSource = rowDef.varIdDef.source
    
    val nonVarIdColumnDefsBySource: Map[CsvSource, Seq[ColumnDef]] = {
      val nonVarIdColumnDefsBySource: Map[CsvSource, Seq[ColumnDef]] = rowDef.otherColumns.groupBy(_.source)
      
      nonVarIdColumnDefsBySource - varIdSource
    }
    
    val withSameSourceAsVarID: Seq[ColumnDef] = nonVarIdColumnDefsBySource.get(varIdSource).getOrElse(Nil)
    
    import Maps.Implicits._
    
    val parseFnsBySourceNonVarId: Map[CsvSource, ParseFn] = nonVarIdColumnDefsBySource.strictMapValues(fuse(flipDetector))
    
    val recordsAndParsersNonVarId: Seq[(Iterator[CsvRow], ParseFn)] = {
      parseFnsBySourceNonVarId.toSeq.map { case (source, parseFn) => (source.records, parseFn) }
    }
    
    val varIdSourceRecords = varIdSource.records
    
    val parseFnForOtherColumnsFromVarIdSource: ParseFn = fuse(flipDetector)(withSameSourceAsVarID) 
    
    val rows: Iterator[DataRow] = new Iterator[DataRow] {
      override def hasNext: Boolean = varIdSourceRecords.hasNext && recordsAndParsersNonVarId.forall { case (records, _) => records.hasNext }
      override def next(): DataRow = {
        def parseNext(varId: String)(t: (Iterator[CsvRow], ParseFn)): DataRow = {
          val (records, parseFn) = t
          
          val record = records.next()
          
          parseFn(varId, record)
        }
        
        val varIdSourceRecord = varIdSourceRecords.next()
        
        val varIdValue = rowDef.varIdDef.getTypedValueFromSource(varIdSourceRecord).raw
        
        val dataRowFromVarIdSource = parseFnForOtherColumnsFromVarIdSource(varIdValue, varIdSourceRecord)
        
        val dataRowFromOtherSources = recordsAndParsersNonVarId.map(parseNext(varIdValue)).foldLeft(DataRow.empty)(_ ++ _)
        
        dataRowFromVarIdSource ++ dataRowFromOtherSources
      }
    }
    
    val header = headerRowFrom(rowDef.columnDefs)
    
    (header, rows)
  }
  
  def process2(flipDetector: FlipDetector)(rowDef: RowDef): (HeaderRow, Iterator[DataRow]) = {
    val bySource = rowDef.columnDefs.groupBy(_.source)
    
    import Maps.Implicits._
    
    //val parseFn = rowDef.parseFn(flipDetector)
    
    val parsingFunctionsBySource: Seq[(CsvSource, CsvRow => DataRow)] = ???//bySource.strictMapValues().toSeq
    
    val header = headerRowFrom(rowDef.columnDefs)
    
    val isToFns = parsingFunctionsBySource.map { case (src, parseFn) => 
      (src.records, parseFn) 
    }
    
    val rows: Iterator[DataRow] = new Iterator[DataRow] {
      override def hasNext: Boolean = isToFns.forall { case (records, _) => records.hasNext }
      override def next(): DataRow = {
        def parseNext(t: (Iterator[CsvRow], CsvRow => DataRow)): DataRow = {
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
