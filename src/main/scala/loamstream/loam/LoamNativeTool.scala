package loamstream.loam

import loamstream.model.LId
import loamstream.util.ValueBox

/** A command line tool specified in a Loam script */
final case class LoamNativeTool private(id: LId)(implicit val graphBox: ValueBox[LoamGraph]) extends LoamTool {

}
