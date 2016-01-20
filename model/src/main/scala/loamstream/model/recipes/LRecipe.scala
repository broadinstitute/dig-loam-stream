package loamstream.model.recipes

import loamstream.model.calls.LPileCall

import scala.language.higherKinds

/**
  * LoamStream
  * Created by oliverr on 12/23/2015.
  */
trait LRecipe {
  def inputs: Seq[LPileCall]
}
