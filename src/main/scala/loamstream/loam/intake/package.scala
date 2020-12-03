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
  
  type PartialRowParser[A] = PartialFunction[DataRow, A]
  
  type RowParser[A] = DataRow => A
  
  type RowPredicate = Predicate[DataRow]
  
  type CloseableRowPredicate = CloseablePredicate[DataRow]
  
  type RowTransform = Transform[DataRow]
  
  type CloseableRowTransform = CloseableTransform[DataRow]
  
  type TaggedRowParser[A] = VariantRow.Tagged => A
  
  type TaggedRowPredicate = Predicate[VariantRow.Tagged]
  
  type CloseableTaggedRowPredicate = CloseablePredicate[VariantRow.Tagged]
  
  type TaggedRowTransform = Transform[VariantRow.Tagged]
  
  type CloseableTaggedRowTransform = CloseableTransform[VariantRow.Tagged] 
    
  type DataRowParser[A] = AggregatorVariantRow => A
  
  type DataRowPredicate = Predicate[AggregatorVariantRow]
  
  type CloseableDataRowPredicate = CloseablePredicate[AggregatorVariantRow]
  
  type DataRowTransform = Transform[AggregatorVariantRow]
  
  type CloseableDataRowTransform = CloseableTransform[AggregatorVariantRow]
}
