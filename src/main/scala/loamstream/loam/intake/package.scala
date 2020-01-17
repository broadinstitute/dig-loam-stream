package loamstream.loam

import org.apache.commons.csv.CSVRecord


/**
 * @author clint
 * Dec 17, 2019
 */
package object intake {
  type PartialRowParser[A] = PartialFunction[CSVRecord, A]
  
  type RowParser[A] = CSVRecord => A
  
  type RowPredicate = RowParser[Boolean]
  
  type ParseFn = (String, CSVRecord) => DataRow
}
