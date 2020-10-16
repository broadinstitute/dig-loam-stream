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
  
  final val index: Int = ColumnDef.nextColumnIndex()
  
  final def dataType: DataType = expr.dataType
  final def dataTypeFlipped: Option[DataType] = exprWhenFlipped.map(_.dataType)
  
  final def getTypedValueFromSource: RowParser[TypedData] = { row =>
    val rawValue = expr(row)
    
    TypedData(rawValue.toString, dataType)
  }
  
  final def getTypedValueFromSourceWhenFlipNeeded: RowParser[TypedData] = { row =>
    val exprToInvoke: ColumnExpr[_] = exprWhenFlipped.getOrElse(expr)
    
    val rawValue = exprToInvoke(row).toString
    val dt = dataTypeFlipped.getOrElse(dataType)
    
    TypedData(rawValue, dt)
  }
  
  def withName(name: ColumnName): NamedColumnDef[A] = NamedColumnDef(name, expr, exprWhenFlipped)
}

object ColumnDef {
  private[this] val indices: Sequence[Int] = Sequence()
  
  private[intake] def nextColumnIndex(): Int = indices.next()
}
