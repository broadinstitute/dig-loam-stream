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
  
  type PartialDataRowParser[A] = PartialFunction[DataRow, A]
  
  type DataRowParser[A] = DataRow => A
  
  type DataRowPredicate = Predicate[DataRow]
  
  type CloseableDataRowPredicate = CloseablePredicate[DataRow]
  
  type DataRowTransform = Transform[DataRow]
  
  type CloseableDataRowTransform = CloseableTransform[DataRow]
  
  type TaggedRowParser[A] = VariantRow.Tagged => A
  
  type TaggedRowPredicate = Predicate[VariantRow.Tagged]
  
  type CloseableTaggedRowPredicate = CloseablePredicate[VariantRow.Tagged]
  
  type TaggedRowTransform = Transform[VariantRow.Tagged]
  
  type CloseableTaggedRowTransform = CloseableTransform[VariantRow.Tagged] 
    
  type AggregatorVariantRowParser[A] = AggregatorVariantRow => A
  
  type AggregatorVariantRowPredicate = Predicate[AggregatorVariantRow]
  
  type CloseableAggregatorVariantRowPredicate = CloseablePredicate[AggregatorVariantRow]
  
  type AggregatorVariantRowTransform = Transform[AggregatorVariantRow]
  
  type CloseableAggregatorVariantRowTransform = CloseableTransform[AggregatorVariantRow]
}
