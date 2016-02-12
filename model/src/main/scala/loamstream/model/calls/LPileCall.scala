package loamstream.model.calls

import loamstream.model.kinds.LKind
import loamstream.model.recipes.LRecipe

import scala.reflect.runtime.universe.Type

/**
  * LoamStream
  * Created by oliverr on 12/23/2015.
  */

trait LPileCall {
  def keyTypes: Seq[Type]

  def kind: LKind

  def recipe: LRecipe

  def extractKey(index: Int, kind: LKind) = LSetCall(Seq(keyTypes(index)), LRecipe.ExtractKey(this, index), kind)
}
