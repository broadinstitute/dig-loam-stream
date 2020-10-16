package loamstream.loam.intake.aggregator

import loamstream.util.LogContext
import loamstream.loam.intake.CsvRow
import loamstream.util.Options

/**
 * @author clint
 * Oct 14, 2020
 */
object RowFilters {
  /*
   * automatically skip variants with missing values in any used column
   * 0 < eaf < 1
   * 0 < maf <= 0.5
   * 0 <= p <= 1 (reset p values of 0 to minimum representable value)
   * warning resulting from p-values of 1: /humgen/diabetes2/users/ryank/software/dig-aggregator-intake/processors/variants.py:22: RuntimeWarning: divide by zero encountered in true_divide
   * return 1.0 if beta == 0 else -numpy.divide(abs(beta), scipy.stats.norm.ppf(p / 2))
   *
   */
  
  def validEaf(implicit logCtx: LogContext): DataRowPredicate = { row =>
    row.eaf match {
      case Some(eaf) => {
        val valid = (eaf > 0.0) && (eaf < 1.0)
    
        if(!valid) {
          logCtx.warn(s"Row has invalid EAF (${eaf}): '${row}'")
        }
        
        valid
      }
      case None => true //TODO ???
    }
  }
  
  def validMaf(implicit logCtx: LogContext): DataRowPredicate = { row =>
    row.maf match {
      case Some(maf) => {
        val valid = (maf > 0.0) && (maf <= 0.5)
    
        if(!valid) {
          logCtx.warn(s"Row has invalid MAF (${maf}): '${row}'")
        }
        
        valid
      }
      case None => true //TODO ???
    }
  }
  
  def validPValue(implicit logCtx: LogContext): DataRowPredicate = { row =>
    import row.pvalue
    
    //TODO: Is 1.0 valid?  Is 0.0?
    val valid = (pvalue > 0.0) && (pvalue < 1.0)
    
    if(!valid) {
      logCtx.warn(s"Row has invalid P-value (${pvalue}): '${row}'")
    }
    
    valid
  }
}
