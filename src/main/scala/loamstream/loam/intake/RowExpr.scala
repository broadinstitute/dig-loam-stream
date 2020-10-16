package loamstream.loam.intake

/**
 * @author clint
 * Oct 13, 2020
 */
trait RowExpr[A] extends (TaggedRowParser[A]) { self  =>
  def eval(input: CsvRow.WithFlipTag): A = self.apply(input)
}

object RowExpr {
  def apply(f: CsvRow.WithFlipTag => CsvRow): RowExpr[CsvRow] = new RowExpr[CsvRow] {
    override def apply(input: CsvRow.WithFlipTag): CsvRow = f(input)
  }
}
