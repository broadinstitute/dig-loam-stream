package loamstream.ideas

import loamstream.model.StoreSpec
import loamstream.model.LId
import loamstream.model.kinds.LKind
import loamstream.model.ToolSpec

/**
 * @author clint
 * date: May 12, 2016
 */
final case class AstSpec(inputs: Map[LId, StoreSpec], outputs: Map[LId, StoreSpec]) {
  override def toString: String = {
    def toStr(m: Map[LId, StoreSpec]): String = {
      m.map { case (id, spec) => s"$id: $spec" }.mkString("{",",","}")
    }
    
    s"${toStr(inputs)} => ${toStr(outputs)}}"
  }
  
  //TODO: What if ordering of inputs and outputs matters
  def toToolSpec(kind: LKind): ToolSpec = ToolSpec(kind, inputs, outputs)
}