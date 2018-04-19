package loamstream.wdl

import loamstream.model.execute.Executable
import loamstream.wdl.model.{WdlElement, Workflow}

object LoamToWdl {

  def loamToWdl(executable: Executable): WdlElement = Workflow("workflow42")

}
