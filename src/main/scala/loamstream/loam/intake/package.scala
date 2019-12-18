package loamstream.loam

import org.apache.commons.csv.CSVRecord


/**
 * @author clint
 * Dec 17, 2019
 */
package object intake {
  type RowParser[A] = CSVRecord => A
  
  type ParseFn = CSVRecord => DataRow
}
