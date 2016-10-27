package loamstream.loam

import loamstream.compiler.LoamCompiler
import loamstream.loam.files.LoamFileManager
import loamstream.util.{DepositBox, ValueBox}

/** Container for compile time and run time context for a project */
final class LoamProjectContext(val graphBox: ValueBox[LoamGraph]) {

  def graph: LoamGraph = graphBox.value

  val fileManager: LoamFileManager = new LoamFileManager
}

/** Container for compile time and run time context for a project */
object LoamProjectContext {

  val depositBox = DepositBox.empty[LoamProjectContext]

  def empty: LoamProjectContext = new LoamProjectContext(ValueBox(LoamGraph.empty))

}