package loamstream.loam

/**
 * @author clint
 * Dec 17, 2019
 */
package object intake {
  type PartialRowParser[A] = PartialFunction[CsvRow, A]
  
  type RowParser[A] = CsvRow => A
  
  type RowPredicate = RowParser[Boolean]
  
  type RowTransform = RowParser[CsvRow]
  
  type TaggedRowParser[A] = CsvRow.WithFlipTag => A
  
  type TaggedRowPredicate = TaggedRowParser[Boolean]
  
  type TaggedRowTransform = TaggedRowParser[CsvRow]
  
  implicit final class RowPredicateOps(val p: RowPredicate) extends AnyVal {
    def ifFailure[A](body: RowParser[A]): RowPredicate = { row =>
      val result = p(row)
      
      if(!result) {
        body(row)
      }
      
      result
    }
  }
  
  type ParseFn = (String, NamedColumnDef[_], CsvRow) => DataRow
}
