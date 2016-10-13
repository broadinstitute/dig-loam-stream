package loamstream.loam

import loamstream.util.DepositBox

/** Container for compile time and run time context for a script */
class LoamScriptContext(val projectContext: LoamProjectContext) {

}

/** Container for compile time and run time context for a script */
object LoamScriptContext {
  def fromDepositedProjectContext(receipt: DepositBox.Receipt): LoamScriptContext =
    new LoamScriptContext(LoamProjectContext.depositBox(receipt))
}