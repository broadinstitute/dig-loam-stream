package loamstream.loam.intake

import loamstream.util.Sequence


/**
 * @author clint
 * Dec 17, 2019
 */
trait ColumnDef {
  def name: ColumnName
  def getValueFromSource: RowParser[Any]
  def getValueFromSourceWhenFlipNeeded: Option[RowParser[Any]]
  def index: Int
}

object ColumnDef {
  def apply(
    name: ColumnName, 
    getValueFromSource: RowParser[Any],
    getValueFromSourceWhenFlipNeeded: RowParser[Any]): UnsourcedColumnDef = {
    
    new UnsourcedColumnDef(name, getValueFromSource, Some(getValueFromSourceWhenFlipNeeded))
  }
  
  def apply(
    name: String, 
    srcColumn: RowParser[Any],
    srcColumnWhenFlipNeeded: RowParser[Any]): UnsourcedColumnDef = {
    
    apply(ColumnName(name), srcColumn, srcColumnWhenFlipNeeded)
  }
  
  def apply(name: String, srcColumn: RowParser[Any]): UnsourcedColumnDef = apply(ColumnName(name), srcColumn)
  
  def apply(name: ColumnName, srcColumn: RowParser[Any]): UnsourcedColumnDef = {
    new UnsourcedColumnDef(name, srcColumn, None)
  }
  
  def apply(name: ColumnName): UnsourcedColumnDef = apply(name, name, name)
  
  private[this] val indices: Sequence[Int] = Sequence()
  
  private[intake] def nextColumnIndex(): Int = indices.next()
}

final case class UnsourcedColumnDef(
    name: ColumnName, 
    getValueFromSource: RowParser[Any],
    getValueFromSourceWhenFlipNeeded: Option[RowParser[Any]] = None) extends ColumnDef {
  
  override val index: Int = ColumnDef.nextColumnIndex()
  
  //def from(source: CsvSource): SourcedColumnDef = source.producing(Seq(this))
}

/**
 * @author clint
 * Dec 19, 2019
 */
final case class SourcedColumnDef(columnDef: ColumnDef, source: CsvSource) extends ColumnDef {
  override def name: ColumnName = columnDef.name
  override def getValueFromSource: RowParser[Any] = columnDef.getValueFromSource
  override def getValueFromSourceWhenFlipNeeded: Option[RowParser[Any]] = columnDef.getValueFromSourceWhenFlipNeeded
  override def index: Int = columnDef.index
}
 
