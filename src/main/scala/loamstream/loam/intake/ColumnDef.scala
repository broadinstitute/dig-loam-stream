package loamstream.loam.intake

import loamstream.util.Sequence

/**
 * @author clint
 * Oct 15, 2020
 */
trait ColumnDef[A] extends (CsvRow.WithFlipTag => A) {
  
  override def apply(row: CsvRow.WithFlipTag): A = {
    val exprToInvoke: ColumnExpr[A] = {
      def whenFlipped: ColumnExpr[A] = exprWhenFlipped match {
        case Some(e) => e
        case None => expr
      }
      
      if(row.isFlipped) whenFlipped else expr
    }
    
    exprToInvoke(row)
  }
  
  def expr: ColumnExpr[A]
  
  def exprWhenFlipped: Option[ColumnExpr[A]]
  
  def withName(name: ColumnName): NamedColumnDef[A] = NamedColumnDef(name, expr, exprWhenFlipped)
}
