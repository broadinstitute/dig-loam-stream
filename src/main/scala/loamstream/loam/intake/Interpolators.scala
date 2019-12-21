package loamstream.loam.intake


/**
 * @author clint
 * Dec 17, 2019
 */
object Interpolators extends Interpolators

trait Interpolators {
  final implicit class StringOps(val stringContext: StringContext) {
    def strexpr(args: Any*): RowParser[String] = { row =>
      val stringArgs: Seq[String] = args.map {
        case columnExpr: ColumnExpr[_] => columnExpr.render(row)
        case a => a.toString
      }
      
      //TODO: handle case where there are no parts (can that happen? expr"" ?)
      val firstPart +: stringParts = stringContext.parts
      
      val allParts: Seq[String] = firstPart +: {
        stringParts.zip(stringArgs).flatMap { case (stringPart, arg) =>
          Seq(arg, stringPart)
        }
      }
      
      allParts.mkString("")
    }
  }
}
