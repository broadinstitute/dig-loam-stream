package loamstream.loam.intake

import loamstream.model.Store
import loamstream.util.LogContext
import java.io.Closeable

/**
 * @author clint
 * Oct 14, 2020
 */
trait RowTransforms { self: IntakeSyntax =>
  /*
   * automatically skip variants with missing values in any used column
   * 0 < eaf < 1
   * 0 < maf <= 0.5
   * 0 <= p <= 1 (reset p values of 0 to minimum representable value)
   * warning resulting from p-values of 1: dig-aggregator-intake/processors/variants.py:22: 
   * RuntimeWarning: divide by zero encountered in true_divide
   * return 1.0 if beta == 0 else -numpy.divide(abs(beta), scipy.stats.norm.ppf(p / 2))
   *
   */
  object DataRowTransforms {
    def clampPValues(logStore: Store, append: Boolean = false): CloseableAggregatorVariantRowTransform = {
      clampPValues(Log.toFile(logStore, append))
    }
    
    def clampPValues(implicit logCtx: ToFileLogContext): CloseableAggregatorVariantRowTransform = {
      RowTransforms.ConcreteCloseableTransform[AggregatorVariantRow](logCtx) { row =>
        import row.pvalue
        
        val mungedPValue = if(pvalue == 0.0) {
          val newPValue = Double.MinPositiveValue
          
          logCtx.warn {
            val variantId = row.marker.underscoreDelimited
            
            s"${s"Variant ${variantId} has invalid P-value (${pvalue}), clamped to '${newPValue}'"}"
          }
          
          newPValue
        } else {
          pvalue
        }
        
        row.copy(pvalue = mungedPValue)
      }
    }
    
    def upperCaseAlleles: AggregatorVariantRowTransform = { row =>
      row.copy(marker = row.marker.toUpperCase)
    }
  }
}

object RowTransforms {
  final class ConcreteCloseableTransform[A](toClose: Closeable)(t: Transform[A]) extends Transform[A] with Closeable {
    override def apply(a: A): A = t(a)
    
    override def close(): Unit = toClose.close()
  }
  
  object ConcreteCloseableTransform {
    def apply[A](toClose: Closeable)(p: Transform[A]): ConcreteCloseableTransform[A] = {
      new ConcreteCloseableTransform(toClose)(p)
    }
  }
}
