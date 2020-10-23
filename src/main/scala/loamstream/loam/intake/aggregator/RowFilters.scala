package loamstream.loam.intake.aggregator

import loamstream.util.LogContext
import loamstream.loam.intake.CsvRow
import loamstream.util.Options
import java.nio.file.Path
import loamstream.model.Store
import loamstream.loam.intake.RowPredicate
import loamstream.loam.intake.ColumnExpr
import loamstream.loam.intake.IntakeSyntax

/**
 * @author clint
 * Oct 14, 2020
 */
trait RowFilters { self: IntakeSyntax => 
  object CsvRowFilters {
    private val di: Set[String] = Set("D", "I")
    
    def noDsNorIs(
        refColumn: ColumnExpr[String], 
        altColumn: ColumnExpr[String],
        logStore: Store,
        append: Boolean = false): RowPredicate = noDsNorIs(refColumn, altColumn)(Log.toFile(logStore, append))
    
    def noDsNorIs(
        refColumn: ColumnExpr[String], 
        altColumn: ColumnExpr[String])(implicit logCtx: LogContext): RowPredicate = {
      
      filterRefAndAlt(refColumn, altColumn, di)
    }
    
    def filterRefAndAlt(
        refColumn: ColumnExpr[String], 
        altColumn: ColumnExpr[String],
        disallowed: Set[String],
        logStore: Store,
        append: Boolean = false): RowPredicate = {
      
      filterRefAndAlt(refColumn, altColumn, disallowed)(Log.toFile(logStore, append))
    }
    
    def filterRefAndAlt(
        refColumn: ColumnExpr[String], 
        altColumn: ColumnExpr[String],
        disallowed: Set[String])(implicit logCtx: LogContext): RowPredicate = {
      
      def isAllowed(s: String): Boolean = !disallowed.contains(s)
      
      row => isAllowed(refColumn(row)) && isAllowed(altColumn(row))
    }
    
    def logToFile(store: Store, append: Boolean = false)(p: RowPredicate): RowPredicate = { 
      doLogToFile[CsvRow](store, append)(p)
    }
  }
  
  object DataRowFilters {
    def logToFile(store: Store, append: Boolean = false)(p: DataRowPredicate): DataRowPredicate = { 
      doLogToFile[DataRow](store, append)(p)
    }
    
    /*
     * automatically skip variants with missing values in any used column
     * 0 < eaf < 1
     * 0 < maf <= 0.5
     * 0 <= p <= 1 (reset p values of 0 to minimum representable value)
     * warning resulting from p-values of 1: /humgen/diabetes2/users/ryank/software/dig-aggregator-intake/processors/variants.py:22: RuntimeWarning: divide by zero encountered in true_divide
     * return 1.0 if beta == 0 else -numpy.divide(abs(beta), scipy.stats.norm.ppf(p / 2))
     *
     */
    
    def validEaf(logTo: Store, append: Boolean = false): DataRowPredicate = validEaf(Log.toFile(logTo, append))
    
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
    
    def validMaf(logTo: Store, append: Boolean = false): DataRowPredicate = validMaf(Log.toFile(logTo, append))
    
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
    
    def validPValue(logTo: Store, append: Boolean = false): DataRowPredicate = validPValue(Log.toFile(logTo, append))
    
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
  
  private def defaultMessage[R](r: R): String = s"Skipping row '${r}'"
  
  private def doLogToFile[R](
      store: Store, 
      append: Boolean = false,
      makeMessage: R => String = defaultMessage(_: R))(p: R => Boolean): R => Boolean = {
        
    val logCtx = Log.toFile(store, append)
      
    row => {
      val result = p(row)
      
      if(!result) {
        logCtx.warn(makeMessage(row))
      }
      
      result
    }
  }
}
