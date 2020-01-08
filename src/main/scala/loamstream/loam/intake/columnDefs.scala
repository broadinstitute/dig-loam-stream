package loamstream.loam.intake

import loamstream.util.Sequence


/**
 * @author clint
 * Dec 17, 2019
 */
trait ColumnDef {
  def name: ColumnName
  def getValueFromSource: ColumnExpr[_]
  def getValueFromSourceWhenFlipNeeded: Option[ColumnExpr[_]]
  def index: Int
  
  final def dataType: DataType = getValueFromSource.dataType
  
  final def getTypedValueFromSource: RowParser[TypedData] = { row =>
    val rawValue = getValueFromSource(row)
    
    TypedData(rawValue.toString, dataType)
  }
}

object ColumnDef {
  def apply(
    name: ColumnName, 
    getValueFromSource: ColumnExpr[_],
    getValueFromSourceWhenFlipNeeded: ColumnExpr[_]): UnsourcedColumnDef = {
    
    new UnsourcedColumnDef(name, getValueFromSource, Some(getValueFromSourceWhenFlipNeeded))
  }
  
  def apply(
    name: String, 
    srcColumn: ColumnExpr[_],
    srcColumnWhenFlipNeeded: ColumnExpr[_]): UnsourcedColumnDef = {
    
    apply(ColumnName(name), srcColumn, srcColumnWhenFlipNeeded)
  }
  
  def apply(name: String, srcColumn: ColumnExpr[_]): UnsourcedColumnDef = apply(ColumnName(name), srcColumn)
  
  def apply(name: ColumnName, srcColumn: ColumnExpr[_]): UnsourcedColumnDef = {
    new UnsourcedColumnDef(name, srcColumn, None)
  }
  
  def apply(name: ColumnName): UnsourcedColumnDef = apply(name, name, name)
  
  private[this] val indices: Sequence[Int] = Sequence()
  
  private[intake] def nextColumnIndex(): Int = indices.next()
}

final case class UnsourcedColumnDef(
    name: ColumnName, 
    getValueFromSource: ColumnExpr[_],
    getValueFromSourceWhenFlipNeeded: Option[ColumnExpr[_]] = None) extends ColumnDef {
  
  override val index: Int = ColumnDef.nextColumnIndex()
  
  def from(source: CsvSource): SourcedColumnDef = source.producing(this)
}

/**
 * @author clint
 * Dec 19, 2019
 */
final case class SourcedColumnDef(columnDef: ColumnDef, source: CsvSource) extends ColumnDef {
  override def name: ColumnName = columnDef.name
  override def getValueFromSource: ColumnExpr[_] = columnDef.getValueFromSource
  override def getValueFromSourceWhenFlipNeeded: Option[ColumnExpr[_]] = columnDef.getValueFromSourceWhenFlipNeeded
  override def index: Int = columnDef.index
}
 
