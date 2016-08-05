package loamstream.loam

import loamstream.model.{LId, Store}
import loamstream.util.ValueBox

/**
  * LoamStream - Language for Omics Analysis Management
  * Created by oruebenacker on 8/5/16.
  */
/** A command line tool specified in a Loam script */
final case class LoamNativeTool private(id: LId)(implicit val graphBox: ValueBox[LoamGraph]) extends LoamTool {
  override def inputs: Map[LId, Store] = ??? // TODO

  override def outputs: Map[LId, Store] = ??? // TODO
}
