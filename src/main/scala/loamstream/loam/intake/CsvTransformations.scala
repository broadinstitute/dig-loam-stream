package loamstream.loam.intake

/**
 * @author clint
 * Apr 9, 2020
 * 
 * Methods that do the actual processing of data from CsvSources via ColumnDefs and RowDefs
 */
trait CsvTransformations { self: IntakeSyntax =>

  private[intake] def headerRowFrom(columnDefs: Seq[NamedColumnDef[_]]): HeaderRow = {
    HeaderRow(columnDefs.sortBy(_.name.index).map(cd => (cd.name.name, cd.expr.dataType)))
  }

}
