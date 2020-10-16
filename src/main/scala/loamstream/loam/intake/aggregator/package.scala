package loamstream.loam.intake

/**
 * @author clint
 * Oct 15, 2020
 */
package object aggregator {
  type DataRowParser[A] = DataRow => A
  
  type DataRowPredicate = DataRowParser[Boolean]
  
  type DataRowTransform = DataRowParser[DataRow]
}