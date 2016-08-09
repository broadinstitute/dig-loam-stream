package loamstream.loam

import loamstream.model.LId
import loamstream.util.{EvalLaterBox, ValueBox}
import scala.reflect.runtime.universe.TypeTag

/** A native tool specified in a Loam script */
final case class LoamNativeTool[T] private(id: LId, defaultStores: Set[LoamStore], expBox: EvalLaterBox[T])
                                          (implicit val graphBox: ValueBox[LoamGraph]) extends LoamTool

object LoamNativeTool {
  def apply[T: TypeTag](defaultStores: Set[LoamStore], expr: => T)(
    implicit graphBox: ValueBox[LoamGraph]): LoamNativeTool[T] = {
    val tool = LoamNativeTool(LId.newAnonId, defaultStores, EvalLaterBox(expr))
    graphBox(_.withTool(tool))
    tool
  }
}


