package loamstream.model.recipes

import loamstream.model.calls.LPileCall

/**
  * LoamStream
  * Created by oliverr on 1/5/2016.
  */
case class LCheckoutPreexisting(id: String) extends LRecipe {
  override def inputs = Seq.empty[LPileCall]
}
