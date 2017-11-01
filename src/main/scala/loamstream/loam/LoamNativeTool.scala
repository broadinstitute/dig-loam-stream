package loamstream.loam

import loamstream.model.Tool.{AllStores, DefaultStores, InputsAndOutputs}
import loamstream.model.{LId, Store, Tool}
import loamstream.util.EvalLaterBox

import scala.reflect.runtime.universe.TypeTag

/** A native tool specified in a Loam script */
final case class LoamNativeTool[T] private (
    id: LId, 
    defaultStores: DefaultStores, 
    expBox: EvalLaterBox[T])(implicit val scriptContext: LoamScriptContext) extends Tool

object LoamNativeTool {
  def apply[T: TypeTag](defaultStores: DefaultStores,
                        expr: => T)(implicit scriptContext: LoamScriptContext): LoamNativeTool[T] = {

    val tool = LoamNativeTool(LId.newAnonId, defaultStores, EvalLaterBox(expr))
    
    scriptContext.projectContext.updateGraph(_.withTool(tool, scriptContext))
    
    tool
  }

  def apply[T: TypeTag](
      defaultStores: Set[Store],
      expr: => T)(implicit scriptContext: LoamScriptContext): LoamNativeTool[T] = apply(AllStores(defaultStores), expr)

  def apply[T: TypeTag](
      inStores: Set[Store],
      outStores: Set[Store],
      expr: => T)(implicit scriptContext: LoamScriptContext): LoamNativeTool[T] = {
    
    apply(InputsAndOutputs(inStores, outStores), expr)
  }

  def apply[T: TypeTag](
      in: Tool.In,
      out: Tool.Out, expr: => T)(implicit scriptContext: LoamScriptContext): LoamNativeTool[T] = {
    
    apply(InputsAndOutputs(in, out), expr)
  }

  def apply[T: TypeTag](
      in: Tool.In,
      expr: => T)(implicit scriptContext: LoamScriptContext): LoamNativeTool[T] = apply(InputsAndOutputs(in), expr)

  def apply[T: TypeTag](
      out: Tool.Out,
      expr: => T)(implicit scriptContext: LoamScriptContext): LoamNativeTool[T] = apply(InputsAndOutputs(out), expr)
}


