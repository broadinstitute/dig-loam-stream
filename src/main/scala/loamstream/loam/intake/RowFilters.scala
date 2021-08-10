package loamstream.loam.intake

import loamstream.model.Store

/**
 * @author clint
 * Oct 14, 2020
 */
object RowFilters {
  private[intake] def asString(r: RenderableRow): String = r.headers.iterator.zip(r.values.iterator).map { 
    case (h, Some(v)) => s"(${h},${v})"
    case (h, None) => s"(${h},.)"
  }.mkString(",")
}

trait RowFilters { self: IntakeSyntax =>
  import RowFilters._
  
  object DataRowFilters {
    /**
     * Pass rows with only allowed alleles (BPs, sequences of BPs, optionally comma-seperated)
     */
    def hasAllowedAlleles(
        refColumn: ColumnExpr[String], 
        altColumn: ColumnExpr[String],
        logStore: Store,
        append: Boolean = false): CloseableDataRowPredicate = {
      
      hasAllowedAlleles(refColumn, altColumn)(Log.toFile(logStore, append))
    }

    /**
     * Pass rows with only allowed alleles (BPs, sequences of BPs, optionally comma-seperated)
     */
    def hasAllowedAlleles(
      refColumn: ColumnExpr[String], 
      altColumn: ColumnExpr[String])(implicit logCtx: ToFileLogContext): CloseableDataRowPredicate = {
      
      filterRefAndAlt(refColumn, altColumn, Alleles.areAllowedAlleles)(logCtx)
    }

    private val di: Set[String] = Set("D", "I")
    
    /**
     * Pass rows with no 'D's or 'I's in either refColumn or altColumn
     */
    def noDsNorIs(
        refColumn: ColumnExpr[String], 
        altColumn: ColumnExpr[String],
        logStore: Store,
        append: Boolean = false): CloseableDataRowPredicate = {

      noDsNorIs(refColumn, altColumn)(Log.toFile(logStore, append))
    }
    
    /**
     * Pass rows with no 'D's or 'I's in either refColumn or altColumn
     */
    def noDsNorIs(
        refColumn: ColumnExpr[String], 
        altColumn: ColumnExpr[String])(implicit logCtx: ToFileLogContext): CloseableDataRowPredicate = {
      
      filterRefAndAlt(refColumn, altColumn, !di.contains(_))(logCtx)
    }
    
    /**
     * Pass rows with no disallowed values in either refColumn or altColumn
     */
    def filterRefAndAlt(
        refColumn: ColumnExpr[String], 
        altColumn: ColumnExpr[String],
        p: String => Boolean,
        logStore: Store,
        append: Boolean = false): CloseableDataRowPredicate = {
      
      filterRefAndAlt(refColumn, altColumn, p)(Log.toFile(logStore, append))
    }
    
    /**
     * Pass rows with no disallowed values in either refColumn or altColumn
     */
    def filterRefAndAlt(
        refColumn: ColumnExpr[String], 
        altColumn: ColumnExpr[String],
        p: String => Boolean)(implicit logCtx: ToFileLogContext): CloseableDataRowPredicate = {
      
      ConcreteCloseablePredicate[DataRow](logCtx) { row => 
        val valid = p(refColumn(row)) && p(altColumn(row))
        
        if(!valid) {
          logCtx.warn {
            s"Row #${row.recordNumber} ${asString(row)} contains a disallowed value in $refColumn or $altColumn "
          }
        }
        
        valid
      }
    }
    
    /**
     * Given a RowPredicate and a place to log rows that don't pass the predicate, return a CloseableRowPredicate
     * That applies `p` and writes a message for each row that doesn't pass to `store`.
     */
    def logToFile(
        store: Store, 
        append: Boolean = false, 
        makeMessage: DataRow => String = defaultMessage(_))
       (p: DataRowPredicate): CloseableDataRowPredicate = {
      
      doLogToFile[DataRow](store, append, makeMessage)(p)
    }
  }
  
  object AggregatorVariantRowFilters {
    /**
     * Given a DataRowPredicate and a place to log rows that don't pass the predicate, return a 
     * CloseableDataRowPredicate That applies `p` and writes a message for each row that doesn't pass to `store`.
     */
    def logToFile(
        store: Store, 
        append: Boolean = false, 
        makeMessage: BaseVariantRow => String = defaultMessage(_))
       (p: PValueVariantRowPredicate): CloseablePValueVariantRowPredicate = {
      
      doLogToFile[PValueVariantRow](store, append, makeMessage)(p)
    }
    
    private def rowNumberPart(row: BaseVariantRow): String = {
      val numberPart = row.derivedFromRecordNumber match {
        case Some(n) => n.toString
        case _ => "<unknown>"
      }
      
      s" (from row #${numberPart}) "
    }
    
    /**
     * Pass rows where 0 < eaf < 1
     */
    def validEaf(logTo: Store, append: Boolean = false): CloseablePValueVariantRowPredicate = {
      validEaf(Log.toFile(logTo, append))
    }
    
    /**
     * Pass rows where 0 < eaf < 1
     */
    def validEaf(implicit logCtx: ToFileLogContext): CloseablePValueVariantRowPredicate = { 
      ConcreteCloseablePredicate[PValueVariantRow](logCtx) { row =>
        row.eaf match {
          case Some(eaf) => {
            val valid = (eaf > 0.0) && (eaf < 1.0)
        
            if(!valid) {
              logCtx.warn(s"Variant ${row.marker.underscoreDelimited}${rowNumberPart(row)}" +
                          s"has invalid EAF (${eaf}): '${asString(row)}'")
            }
            
            valid
          }
          case None => true //TODO ???
        }
      }
    }
    
    /**
     * Pass rows where 0 < maf <= 0.5
     */
    def validMaf(logTo: Store, append: Boolean = false): CloseablePValueVariantRowPredicate = {
      validMaf(Log.toFile(logTo, append))
    }
    
    /**
     * Pass rows where 0 < maf <= 0.5
     */
    def validMaf(implicit logCtx: ToFileLogContext): CloseablePValueVariantRowPredicate = {
      ConcreteCloseablePredicate[PValueVariantRow](logCtx) { row =>
        row.maf match {
          case Some(maf) => {
            val valid = (maf > 0.0) && (maf <= 0.5)
        
            if(!valid) {
              logCtx.warn(s"Variant ${row.marker.underscoreDelimited}${rowNumberPart(row)}" + 
                          s"has invalid MAF (${maf}): '${asString(row)}'")
            }
            
            valid
          }
          case None => true //TODO ???
        }
      }
    }
    
    /**
     * Pass rows where 0 < p <= 1
     */
    def validPValue(logTo: Store, append: Boolean = false): CloseablePValueVariantRowPredicate = {
      validPValue(Log.toFile(logTo, append))
    }
    
    /**
     * Pass rows where 0 < p <= 1
     */
    def validPValue(implicit logCtx: ToFileLogContext): CloseablePValueVariantRowPredicate = {
      ConcreteCloseablePredicate[PValueVariantRow](logCtx) { row =>
        import row.pvalue
      
        val valid = (pvalue > 0.0) && (pvalue <= 1.0)
        
        if(!valid) {
          logCtx.warn(s"Variant ${row.marker.underscoreDelimited}${rowNumberPart(row)}" +
                      s"has invalid P-value (${pvalue}): '${asString(row)}'")
        }
        
        valid
      }
    }
  }
  
  private def defaultMessage(r: RenderableRow): String = {
    val recordNumberPart = r match {
      case rwrn: RowWithRecordNumber => s" #${rwrn.recordNumber} "
      case _ => " "
    }
    
    s"Skipping row${recordNumberPart}'${asString(r)}'"
  }
  
  private def doLogToFile[R <: RenderableRow](
      store: Store, 
      append: Boolean = false)(p: R => Boolean): CloseablePredicate[R] = {
   
    doLogToFile(store, append, defaultMessage(_: R))(p)
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
