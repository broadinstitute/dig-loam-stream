package loamstream.loam

import loamstream.loam.LoamTool.{AllStores, DefaultStores, InputsAndOutputs}
import loamstream.model.LId
import loamstream.util.EvalLaterBox

import scala.reflect.runtime.universe.TypeTag

/** A native tool specified in a Loam script */
final case class LoamNativeTool[T] private(id: LId, defaultStores: DefaultStores, expBox: EvalLaterBox[T])
                                          (implicit val context: LoamContext) extends LoamTool

object LoamNativeTool {
  def apply[T: TypeTag](defaultStores: DefaultStores, expr: => T)(
    implicit context: LoamContext): LoamNativeTool[T] = {
    val tool = LoamNativeTool(LId.newAnonId, defaultStores, EvalLaterBox(expr))
    context.graphBox.mutate(_.withTool(tool))
    tool
  }

  def apply[T: TypeTag](defaultStores: Set[LoamStore], expr: => T)(
    implicit context: LoamContext): LoamNativeTool[T] =
    apply(AllStores(defaultStores), expr)

  def apply[T: TypeTag](inStores: Set[LoamStore], outStores: Set[LoamStore], expr: => T)(
    implicit context: LoamContext): LoamNativeTool[T] =
    apply(InputsAndOutputs(inStores, outStores), expr)

  def apply[T: TypeTag](in: LoamTool.In, out: LoamTool.Out, expr: => T)(
    implicit context: LoamContext): LoamNativeTool[T] =
    apply(InputsAndOutputs(in, out), expr)

  def apply[T: TypeTag](in: LoamTool.In, expr: => T)(
    implicit context: LoamContext): LoamNativeTool[T] =
    apply(InputsAndOutputs(in), expr)

  def apply[T: TypeTag](out: LoamTool.Out, expr: => T)(
    implicit context: LoamContext): LoamNativeTool[T] =
    apply(InputsAndOutputs(out), expr)
}


