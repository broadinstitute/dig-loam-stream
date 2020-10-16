package loamstream.loam.intake.aggregator

import loamstream.util.LogContext
import loamstream.loam.intake.CsvRow

/**
 * @author clint
 * Oct 14, 2020
 */
object RowTransforms {
  /*
   * automatically skip variants with missing values in any used column
   * 0 < eaf < 1
   * 0 < maf <= 0.5
   * 0 <= p <= 1 (reset p values of 0 to minimum representable value)
   * warning resulting from p-values of 1: /humgen/diabetes2/users/ryank/software/dig-aggregator-intake/processors/variants.py:22: RuntimeWarning: divide by zero encountered in true_divide
   * return 1.0 if beta == 0 else -numpy.divide(abs(beta), scipy.stats.norm.ppf(p / 2))
   *
   */
  
  def clampPValues(implicit logCtx: LogContext): DataRowTransform = { row =>
    import row.pvalue
    
    val mungedPValue = if(pvalue == 0.0) Double.MinPositiveValue else pvalue
    
    row.copy(pvalue = mungedPValue)
  }
}
