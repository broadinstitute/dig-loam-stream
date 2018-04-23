package loamstream.wdl

import wdl.model.draft3.elements.LanguageElement

object WdlPrinter {

  def print(wdlElement: LanguageElement): String = wdlElement.toString

}
