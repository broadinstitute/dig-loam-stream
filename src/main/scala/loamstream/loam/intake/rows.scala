package loamstream.loam.intake

import org.apache.commons.csv.CSVFormat


/**
 * @author clint
 * Dec 17, 2019
 */
sealed trait Row {
  def values: Seq[String]
}

final case class HeaderRow(values: Seq[String]) extends Row

final case class DataRow(valuesByColumn: Map[ColumnDef, String]) extends Row {
  override def values: Seq[String] = {
    val sortedColumnDefs = valuesByColumn.keys.toSeq.sortBy(_.index)
    
    sortedColumnDefs.map(valuesByColumn.apply(_))
  }
  
  def ++(other: DataRow): DataRow = DataRow(valuesByColumn ++ other.valuesByColumn)
}

object DataRow {
  val empty: DataRow = DataRow(Map.empty)
}
