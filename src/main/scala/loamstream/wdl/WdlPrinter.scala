package loamstream.wdl

import loamstream.wdl.model.WdlElement

object WdlPrinter {

  def print(wdlElement: WdlElement): String = wdlElement.toString

}
