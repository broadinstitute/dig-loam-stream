package loamstream.model.recipes

import loamstream.model.piles.LPile

import scala.language.higherKinds

/**
  * LoamStream
  * Created by oliverr on 12/23/2015.
  */
object LRecipe {

  case class ExtractKey(pile: LPile, index: Int) extends LRecipe {
    def inputs = Seq(pile)
  }

}

trait LRecipe {
  def inputs: Seq[LPile]
}
