package loamstream.model.calls

import loamstream.model.recipes.{LCheckoutPreexisting, LPileCalls, LRecipe}

import scala.reflect.runtime.universe.{TypeTag, typeTag}

/**
  * LoamStream
  * Created by oliverr on 12/23/2015.
  */
object LSetCall {
  def getPreexisting[Keys <: LKeys[_, _] : TypeTag](id: String):
  LSetCall[Keys, LPileCalls.LCalls0]
  = apply[Keys, LPileCalls.LCalls0](new LCheckoutPreexisting(id))

  def apply[Keys <: LKeys[_, _] : TypeTag,
  Inputs <: LPileCalls[_, _]](recipe: LRecipe[Inputs]): LSetCall[Keys, Inputs] =
    LSetCall(typeTag[Keys], recipe)
}

case class LSetCall[Keys <: LKeys[_, _],
Inputs <: LPileCalls[_, _]](keysTag: TypeTag[Keys], recipe: LRecipe[Inputs])
  extends LPileCall[Keys, Inputs] {
}
