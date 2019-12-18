package loamstream.loam.intake

import loamstream.util.Sequence


/**
 * @author clint
 * Dec 17, 2019
 */
final case class ColumnDefinition(
    name: ColumnName, 
    getValueFromSource: RowParser[Any],
    getValueFromSourceWhenFlipNeeded: Option[RowParser[Any]],
    source: CsvSource) {
  
  val index: Int = ColumnDefinition.nextColumnIndex()
}
      
object ColumnDefinition {
  def apply(
    name: ColumnName, 
    getValueFromSource: RowParser[Any],
    getValueFromSourceWhenFlipNeeded: RowParser[Any],
    source: CsvSource): ColumnDefinition = {
    
    new  ColumnDefinition(name, getValueFromSource, Some(getValueFromSourceWhenFlipNeeded), source)
  }
  
  def apply(
    name: String, 
    srcColumn: RowParser[Any],
    srcColumnWhenFlipNeeded: RowParser[Any],
    source: CsvSource): ColumnDefinition = {
    
    apply(ColumnName(name), srcColumn, srcColumnWhenFlipNeeded, source)
  }
  
  def apply(
    name: ColumnName, 
    source: CsvSource): ColumnDefinition = {
    
    apply(name, name, name, source)
  }
  
  private[this] val indices: Sequence[Int] = Sequence()
  
  private def nextColumnIndex(): Int = indices.next()
}
