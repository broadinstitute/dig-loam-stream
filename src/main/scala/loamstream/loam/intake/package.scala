package loamstream.loam

/**
 * @author clint
 * Dec 17, 2019
 */
package object intake {
  type Predicate[A] = A => Boolean
  
  type Transform[A] = A => A
  
  type CloseablePredicate[A] = Predicate[A] with java.io.Closeable
  
  type CloseableTransform[A] = Transform[A] with java.io.Closeable
  
  type PartialRowParser[A] = PartialFunction[CsvRow, A]
  
  type RowParser[A] = CsvRow => A
  
  type RowPredicate = Predicate[CsvRow]
  
  type CloseableRowPredicate = CloseablePredicate[CsvRow]
  
  type RowTransform = Transform[CsvRow]
  
  type CloseableRowTransform = CloseableTransform[CsvRow]
  
  type TaggedRowParser[A] = CsvRow.TaggedCsvRow => A
  
  type TaggedRowPredicate = Predicate[CsvRow.TaggedCsvRow]
  
  type CloseableTaggedRowPredicate = CloseablePredicate[CsvRow.TaggedCsvRow]
  
  type TaggedRowTransform = Transform[CsvRow.TaggedCsvRow]
  
  type CloseableTaggedRowTransform = CloseableTransform[CsvRow.TaggedCsvRow] 
    
  type DataRowParser[A] = DataRow => A
  
  type DataRowPredicate = Predicate[DataRow]
  
  type CloseableDataRowPredicate = CloseablePredicate[DataRow]
  
  type DataRowTransform = Transform[DataRow]
  
  type CloseableDataRowTransform = CloseableTransform[DataRow]
}
