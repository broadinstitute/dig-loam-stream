package loamstream.model.calls

import loamstream.model.recipes.LRecipe

import scala.reflect.runtime.universe.Type

/**
  * LoamStream
  * Created by oliverr on 12/23/2015.
  */

trait LPileCall {
  def keyTypes: Seq[Type]

  def recipe: LRecipe

  def extractKey(index: Int) = LSetCall(Seq(keyTypes(index)), LRecipe.ExtractKey(this, index))
}
