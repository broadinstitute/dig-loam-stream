package loamstream.loam

import org.apache.commons.csv.CSVRecord


/**
 * @author clint
 * Dec 17, 2019
 */
package object intake {
  type CsvRow = CSVRecord
  
  type PartialRowParser[A] = PartialFunction[CsvRow, A]
  
  type RowParser[A] = CsvRow => A
  
  type RowPredicate = RowParser[Boolean]
  
  type ParseFn = (String, CsvRow) => DataRow
}
