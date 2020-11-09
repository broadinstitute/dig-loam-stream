package loamstream.loam.intake

import loamstream.util.LogContext
import loamstream.model.Store
import java.io.Closeable
import RowFilters.ConcreteCloseablePredicate

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
        append: Boolean = false): CloseableRowPredicate = noDsNorIs(refColumn, altColumn)(Log.toFile(logStore, append))
    
    def noDsNorIs(
        refColumn: ColumnExpr[String], 
        altColumn: ColumnExpr[String])(implicit logCtx: ToFileLogContext): CloseableRowPredicate = {
      
      filterRefAndAlt(refColumn, altColumn, di)(logCtx)
    }
    
    def filterRefAndAlt(
        refColumn: ColumnExpr[String], 
        altColumn: ColumnExpr[String],
        disallowed: Set[String],
        logStore: Store,
        append: Boolean = false): CloseableRowPredicate = {
      
      filterRefAndAlt(refColumn, altColumn, disallowed)(Log.toFile(logStore, append))
    }
    
    def filterRefAndAlt(
        refColumn: ColumnExpr[String], 
        altColumn: ColumnExpr[String],
        disallowed: Set[String])(implicit logCtx: ToFileLogContext): CloseableRowPredicate = {
      
      def isAllowed(s: String): Boolean = !disallowed.contains(s)
      
      ConcreteCloseablePredicate[CsvRow](logCtx) { row =>  
        val valid = isAllowed(refColumn(row)) && isAllowed(altColumn(row))
        
        if(!valid) {
          logCtx.warn {
            s"Row ${row.values.mkString(",")} contains a disallowed value from ${disallowed} " +
            s"in ${refColumn(row)} or ${altColumn(row)}"
          }
        }
        
        valid
      }
    }
    
    def logToFile(store: Store, append: Boolean = false)(p: RowPredicate): CloseableRowPredicate = { 
      doLogToFile[CsvRow](store, append)(p)(RowFilters.HasValues.CsvRowsHaveValues)
    }
  }
  
  object DataRowFilters {
    def logToFile(store: Store, append: Boolean = false)(p: DataRowPredicate): CloseableDataRowPredicate = { 
      doLogToFile[DataRow](store, append)(p)(RowFilters.HasValues.DataRowsHaveValues)
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
    
    def validEaf(logTo: Store, append: Boolean = false): CloseableDataRowPredicate = {
      validEaf(Log.toFile(logTo, append))
    }
    
    def validEaf(implicit logCtx: ToFileLogContext): CloseableDataRowPredicate = { 
      ConcreteCloseablePredicate[DataRow](logCtx) { row =>
        row.eaf match {
          case Some(eaf) => {
            val valid = (eaf > 0.0) && (eaf < 1.0)
        
            if(!valid) {
              logCtx.warn(s"Variant ${row.marker.underscoreDelimited} has invalid EAF (${eaf}): '${row}'")
            }
            
            valid
          }
          case None => true //TODO ???
        }
      }
    }
    
    def validMaf(logTo: Store, append: Boolean = false): CloseableDataRowPredicate = {
      validMaf(Log.toFile(logTo, append))
    }
    
    def validMaf(implicit logCtx: ToFileLogContext): CloseableDataRowPredicate = {
      ConcreteCloseablePredicate[DataRow](logCtx) { row =>
        row.maf match {
          case Some(maf) => {
            val valid = (maf > 0.0) && (maf <= 0.5)
        
            if(!valid) {
              logCtx.warn(s"Variant ${row.marker.underscoreDelimited} has invalid MAF (${maf}): '${row}'")
            }
            
            valid
          }
          case None => true //TODO ???
        }
      }
    }
    
    def validPValue(logTo: Store, append: Boolean = false): CloseableDataRowPredicate = {
      validPValue(Log.toFile(logTo, append))
    }
    
    def validPValue(implicit logCtx: ToFileLogContext): CloseableDataRowPredicate = {
      ConcreteCloseablePredicate[DataRow](logCtx) { row =>
        import row.pvalue
      
        //TODO: Is 1.0 valid?  Is 0.0?
        val valid = (pvalue > 0.0) && (pvalue < 1.0)
        
        if(!valid) {
          logCtx.warn(s"Variant ${row.marker.underscoreDelimited} has invalid P-value (${pvalue}): '${row}'")
        }
        
        valid
      }
    }
  }
  
  private def defaultMessage[R](r: R)(implicit ev: RowFilters.HasValues[R]): String = {
    val values = ev.values(r)
    
    s"Skipping row '${values.mkString(",")}'"
  }
  
  private def doLogToFile[R](
      store: Store, 
      append: Boolean = false)(p: R => Boolean)(implicit ev: RowFilters.HasValues[R]): CloseablePredicate[R] = {
   
    doLogToFile(store, append, defaultMessage(_: R)(ev))(p)
  }
        
  private def doLogToFile[R](
      store: Store, 
      append: Boolean,
      makeMessage: R => String)(p: R => Boolean): CloseablePredicate[R] = {
        
    val logCtx = Log.toFile(store, append)
    
    ConcreteCloseablePredicate[R](logCtx) { row =>
      val result = p(row)
      
      if(!result) {
        logCtx.warn(makeMessage(row))
      }
      
      result
    }
  }
}

object RowFilters {
  final class ConcreteCloseablePredicate[A](toClose: Closeable)(p: Predicate[A]) extends Predicate[A] with Closeable {
    override def apply(a: A): Boolean = p(a)
    
    override def close(): Unit = toClose.close()
  }
  
  object ConcreteCloseablePredicate {
    def apply[A](toClose: Closeable)(p: Predicate[A]): ConcreteCloseablePredicate[A] = {
      new ConcreteCloseablePredicate(toClose)(p)
    }
  }
  
  trait HasValues[R] {
    def values(r: R): Seq[String]
  }
  
  object HasValues {
    implicit object DataRowsHaveValues extends HasValues[DataRow] {
      override def values(r: DataRow): Seq[String] = r.values  
    }
    
    implicit object CsvRowsHaveValues extends HasValues[CsvRow] {
      override def values(r: CsvRow): Seq[String] = r.values.toIndexedSeq
    }
  }
}
