package loamstream.loam

import loamstream.model.Tool

/**
 * @author clint
 * Sep 17, 2020
 */
trait CanAddPreamble[T <: Tool] {
  def addPreamble(preamble: String, orig: T)(implicit scriptContext: LoamScriptContext): T
}
