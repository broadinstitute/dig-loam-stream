package loamstream.loam.intake


/**
 * @author clint
 * Dec 17, 2019
 */
object Interpolators extends Interpolators

trait Interpolators {
  final implicit class StringOps(val stringContext: StringContext) {
    def strexpr(args: Any*): ColumnExpr[String] = {
      
      val anyColumnExprsThatNeedEvaluating = args.exists {
        case _: LiteralColumnExpr[_] => false
        case _: ColumnExpr[_] => true
        case _ => false
      }
      
      if(!anyColumnExprsThatNeedEvaluating) {
        LiteralColumnExpr(interpolate(args.map(_.toString)))
      } else {
        ColumnExpr.fromRowParser { row =>
          val stringArgs = args.map {
            case columnExpr: ColumnExpr[_] => columnExpr.render(row)
            case a => a.toString
          }

          interpolate(stringArgs)
        }
      }
    }
    
    private def interpolate(stringArgs: Seq[String]): String = {
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
