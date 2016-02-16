package loamstream.model.recipes

import loamstream.model.kinds.{LAnyKind, LKind}
import loamstream.model.kinds.instances.RecipeKinds
import loamstream.model.piles.LPile

import scala.language.higherKinds

/**
  * LoamStream
  * Created by oliverr on 12/23/2015.
  */
object LRecipe {

  case class ExtractKey(pile: LPile, index: Int) extends LRecipe {
    val kind = RecipeKinds.extractFirstKey
    def inputs = Seq(pile)
    def output = ???
  }

}

trait LRecipe {
  def kind: LKind

  def inputs: Seq[LPile]

  def output: LPile
}
