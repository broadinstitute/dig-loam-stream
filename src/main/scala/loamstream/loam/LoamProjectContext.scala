package loamstream.loam

import loamstream.loam.files.LoamFileManager
import loamstream.util.{DepositBox, ValueBox}
import loamstream.conf.LoamConfig

/** Container for compile time and run time context for a project */
final class LoamProjectContext(val config: LoamConfig, val graphBox: ValueBox[LoamGraph]) {

  def graph: LoamGraph = graphBox.value

  val fileManager: LoamFileManager = new LoamFileManager
}

/** Container for compile time and run time context for a project */
object LoamProjectContext {

  val depositBox = DepositBox.empty[LoamProjectContext]

  def empty(config: LoamConfig): LoamProjectContext = new LoamProjectContext(config, ValueBox(LoamGraph.empty))

}
