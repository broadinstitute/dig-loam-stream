package loamstream.model.recipes

import loamstream.model.kinds.LKind
import loamstream.model.kinds.instances.RecipeKinds
import loamstream.model.piles.LPile

import scala.language.higherKinds

/**
  * LoamStream
  * Created by oliverr on 12/23/2015.
  */
object LRecipe {

  case class ExtractKey(input: LPile, output: LPile, index: Int) extends LRecipe {
    val kind = RecipeKinds.extractKey(index)

    def inputs = Seq(input)
  }

}

trait LRecipe {
  def kind: LKind

  def inputs: Seq[LPile]

  def output: LPile
}
