package loamstream.loam

import loamstream.model.LId
import loamstream.util.ValueBox

/** A command line tool specified in a Loam script */
final case class LoamNativeTool private(id: LId)(implicit val graphBox: ValueBox[LoamGraph]) extends LoamTool {

  /** Returns this after adding input stores to this tool */
  override def in(inStore: LoamStore, inStores: LoamStore*): LoamNativeTool = {
    doIn(inStore, inStores)
    this
  }

  /** Returns this after adding output stores to this tool */
  override def out(outStore: LoamStore, outStores: LoamStore*): LoamNativeTool = {
    doOut(outStore, outStores)
    this
  }

}
