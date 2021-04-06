package loamstream.loam

import scala.util.Failure
import scala.util.Success
import scala.util.Try

import loamstream.loam.intake.BaseVariantRow
import loamstream.loam.intake.DataRow
import loamstream.loam.intake.VariantRow



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
    
  type VariantRowTransform = Transform[BaseVariantRow]
  
  type PolyVariantRowTransform[R <: BaseVariantRow] = Transform[R]
  
  type PValueVariantRowParser[A] = PValueVariantRow => A
  
  type PValueVariantRowPredicate = Predicate[PValueVariantRow]
  
  type CloseablePValueVariantRowPredicate = CloseablePredicate[PValueVariantRow]
  
  type PValueVariantRowTransform = Transform[PValueVariantRow]
  
  type CloseablePValueVariantRowTransform = CloseableTransform[PValueVariantRow]
  
  type VariantCountRowParser[A] = VariantCountRow => A
  
  type VariantCountRowPredicate = Predicate[VariantCountRow]
  
  type CloseableVariantCountRowPredicate = CloseablePredicate[VariantCountRow]
  
  type VariantCountRowTransform = Transform[VariantCountRow]
  
  type CloseableVariantCountRowTransform = CloseableTransform[VariantCountRow]
  
  implicit final class CloseablePredicateOps[A](val cp: CloseablePredicate[A]) extends AnyVal {
    def liftToTry: CloseablePredicate[Try[A]] = {
      ConcreteCloseablePredicate[Try[A]](cp) { 
        case Success(a) => cp(a)
        case Failure(_) => false
      }
    }
  }
}
