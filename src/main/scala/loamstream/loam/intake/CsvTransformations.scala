package loamstream.loam.intake

/**
 * @author clint
 * Apr 9, 2020
 * 
 * Methods that do the actual processing of data from CsvSources via ColumnDefs and RowDefs
 */
trait CsvTransformations { self: IntakeSyntax =>
  private[intake] def fuse(flipDetector: FlipDetector)(columnDefs: Seq[ColumnDef]): ParseFn = {
    (varIdValue, varIdDef, row) =>
      val flipDetected = flipDetector.isFlipped(varIdValue)
      
      def getRowParser(columnDef: ColumnDef): RowParser[TypedData] = {
        if(flipDetected) { columnDef.getTypedValueFromSourceWhenFlipNeeded }
        else { columnDef.getTypedValueFromSource }
      }
      
      val dataRowValues: Map[ColumnDef, TypedData] = {
        Map.empty ++ (varIdDef +: columnDefs).map { columnDef =>
          val columnValueFn: RowParser[TypedData] = getRowParser(columnDef)
          
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
    
    val parseFnsBySourceNonVarId: Map[CsvSource, ParseFn] = {
      nonVarIdColumnDefsBySource.strictMapValues(fuse(flipDetector))
    }
    
    val recordsAndParsersNonVarId: Seq[(Iterator[CsvRow], ParseFn)] = {
      parseFnsBySourceNonVarId.toSeq.map { case (source, parseFn) => (source.records, parseFn) }
    }
    
    val varIdSourceRecords = varIdSource.records
    
    val parseFnForOtherColumnsFromVarIdSource: ParseFn = fuse(flipDetector)(columnDefsWithSameSourceAsVarID) 
    
    val rows: Iterator[DataRow] = {
      dataRowIterator(rowDef, varIdSourceRecords, recordsAndParsersNonVarId, parseFnForOtherColumnsFromVarIdSource)
    }
    
    val header = headerRowFrom(rowDef.columnDefs)
    
    (header, rows)
  }
  
  private def dataRowIterator(
      rowDef: RowDef,
      varIdSourceRecords: Iterator[CsvRow],
      recordsAndParsersNonVarId: Seq[(Iterator[CsvRow], ParseFn)],
      parseFnForOtherColumnsFromVarIdSource: ParseFn): Iterator[DataRow] = new Iterator[DataRow] {
    
    override def hasNext: Boolean = {
      varIdSourceRecords.hasNext && recordsAndParsersNonVarId.forall { case (records, _) => records.hasNext }
    }
    
    override def next(): DataRow = {
      import rowDef.varIdDef
      
      def parseNext(varId: String)(t: (Iterator[CsvRow], ParseFn)): DataRow = {
        val (records, parseFn) = t
        
        val record = records.next()
        
        parseFn(varId, varIdDef, record)
      }
      
      val varIdSourceRecord = varIdSourceRecords.next()
      
      val varIdTypedValue = rowDef.varIdDef.getTypedValueFromSource(varIdSourceRecord)
      
      val varIdValue = varIdTypedValue.raw
      
      val dataRowFromVarIdSource = parseFnForOtherColumnsFromVarIdSource(varIdValue, varIdDef, varIdSourceRecord)
      
      val dataRowFromOtherSources = {
        recordsAndParsersNonVarId.map(parseNext(varIdValue)).foldLeft(DataRow.empty)(_ ++ _)
      }
      
      dataRowFromVarIdSource ++ dataRowFromOtherSources
    }
  }
}
