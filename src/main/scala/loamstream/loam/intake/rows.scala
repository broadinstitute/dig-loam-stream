package loamstream.loam.intake

import org.apache.commons.csv.CSVFormat


/**
 * @author clint
 * Dec 17, 2019
 */
sealed trait Row {
  def values: Seq[String]
}

final case class HeaderRow(typedValues: Seq[(String, DataType)]) extends Row {
  override def values: Seq[String] = typedValues.unzip._1
}

final case class DataRow(valuesByColumn: Map[ColumnDef, TypedData]) extends Row {
  override def values: Seq[String] = {
    val sortedColumnDefs = valuesByColumn.keys.toSeq.sortBy(_.index)
    
    sortedColumnDefs.map(valuesByColumn.apply(_).raw)
  }
  
  def ++(other: DataRow): DataRow = DataRow(valuesByColumn ++ other.valuesByColumn)
}

object DataRow {
  def apply(tuples: (ColumnDef, TypedData)*): DataRow = new DataRow(Map(tuples: _*))
  
  val empty: DataRow = DataRow()
}
