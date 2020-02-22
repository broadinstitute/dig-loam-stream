package loamstream.loam.intake

/**
 * @author clint
 * Feb 10, 2020
 */
object Helpers {
  def csvRow(columnNamesToValues: (String, String)*)(implicit discriminator: Int = 1): CsvRow = new CsvRow {
    override def getFieldByName(name: String): String = {
      columnNamesToValues.collectFirst { case (n, v) if name == n => v }.get
    }
  
    override def getFieldByIndex(i: Int): String = columnNamesToValues.unzip._2.apply(i)
  }
}