package loamstream.loam.intake

import loamstream.loam.LoamScriptContext
import loamstream.loam.NativeTool
import loamstream.model.Store
import loamstream.model.Tool
import loamstream.util.Files
import loamstream.util.Hashes
import loamstream.util.TimeUtils
import loamstream.util.Loggable


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
    
  final class ProcessTarget(dest: Store, varIdColumnDef: SourcedColumnDef, otherColumnDefs: SourcedColumnDef*) extends Loggable {
    def using(flipDetector: FlipDetector)(implicit scriptContext: LoamScriptContext): NativeTool = {
      //TODO: How to wire up inputs (if any)?
      val tool = NativeTool {
        TimeUtils.time(s"Producing ${dest.path}", info(_)) {
          val (headerRow, dataRows) = process(flipDetector)(RowDef(varIdColumnDef, otherColumnDefs))
          
          val csvFormat = CsvSource.Defaults.CommonsCsv.Formats.tabDelimitedWithHeaderCsvFormat
          
          val renderer = CommonsCsvRenderer(csvFormat)
          
          val rowsToWrite: Iterator[Row] = Iterator(headerRow) ++ dataRows
          
          Files.writeLinesTo(dest.path)(rowsToWrite.map(renderer.render))
        }
      }.out(dest)
      
      addToGraph(tool)
      
      tool
    }
  }
  
  final class SchemaFileTarget(dest: Store) {
    def from(columnDefs: ColumnDef*)(implicit scriptContext: LoamScriptContext): NativeTool = {
      val headerRow = headerRowFrom(columnDefs)
      
      val csvFormat = CsvSource.Defaults.CommonsCsv.Formats.tabDelimitedWithHeaderCsvFormat
      
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
      }.out(dest)
      
      addToGraph(tool)
      
      tool
    }
  }
  
  final class AggregatorIntakeConfigFileTarget(dest: Store) {
    def from(configData: aggregator.ConfigData)(implicit scriptContext: LoamScriptContext): NativeTool = {
      //TODO: How to wire up inputs (if any)?
      val tool = NativeTool {
        Files.writeTo(dest.path)(configData.asConfigFileContents)
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
  
  def produceAggregatorIntakeConfigFile(dest: Store) = {
    requireFsPath(dest)
    
    new AggregatorIntakeConfigFileTarget(dest)
  }
  
  private[intake] def fuse(flipDetector: FlipDetector)(columnDefs: Seq[ColumnDef]): ParseFn = { (varIdValue, row) =>
    val flipDetected = flipDetector.isFlipped(varIdValue)
    
    val dataRowValues: Map[ColumnDef, TypedData] = {
      Map.empty ++ columnDefs.map { columnDef =>
        val columnValueFn: RowParser[TypedData] = {
          if(flipDetected) { columnDef.getTypedValueFromSourceWhenFlipNeeded }
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
    
    val columnDefsBySource: Map[CsvSource, Seq[ColumnDef]] = rowDef.otherColumns.groupBy(_.source)
    
    val nonVarIdColumnDefsBySource: Map[CsvSource, Seq[ColumnDef]] = columnDefsBySource - varIdSource
    
    val columnDefsWithSameSourceAsVarID: Seq[ColumnDef] = columnDefsBySource.get(varIdSource).getOrElse(Nil)
    
    import loamstream.util.Maps.Implicits._
    
    val parseFnsBySourceNonVarId: Map[CsvSource, ParseFn] = nonVarIdColumnDefsBySource.strictMapValues(fuse(flipDetector))
    
    val recordsAndParsersNonVarId: Seq[(Iterator[CsvRow], ParseFn)] = {
      parseFnsBySourceNonVarId.toSeq.map { case (source, parseFn) => (source.records, parseFn) }
    }
    
    val varIdSourceRecords = varIdSource.records
    
    val parseFnForOtherColumnsFromVarIdSource: ParseFn = fuse(flipDetector)(columnDefsWithSameSourceAsVarID) 
    
    val rows: Iterator[DataRow] = new Iterator[DataRow] {
      override def hasNext: Boolean = varIdSourceRecords.hasNext && recordsAndParsersNonVarId.forall { case (records, _) => records.hasNext }
      override def next(): DataRow = {
        def parseNext(varId: String)(t: (Iterator[CsvRow], ParseFn)): DataRow = {
          val (records, parseFn) = t
          
          val record = records.next()
          
          parseFn(varId, record)
        }
        
        val varIdSourceRecord = varIdSourceRecords.next()
        
        val varIdTypedValue = rowDef.varIdDef.getTypedValueFromSource(varIdSourceRecord)
        
        val varIdValue = varIdTypedValue.raw
        
        val dataRowForVarId = DataRow(rowDef.varIdDef -> varIdTypedValue)
        
        val dataRowFromVarIdSource = parseFnForOtherColumnsFromVarIdSource(varIdValue, varIdSourceRecord)
        
        val dataRowFromOtherSources = recordsAndParsersNonVarId.map(parseNext(varIdValue)).foldLeft(DataRow.empty)(_ ++ _)
        
        dataRowForVarId ++ dataRowFromVarIdSource ++ dataRowFromOtherSources
      }
    }
    
    val header = headerRowFrom(rowDef.columnDefs)
    
    (header, rows)
  }
}