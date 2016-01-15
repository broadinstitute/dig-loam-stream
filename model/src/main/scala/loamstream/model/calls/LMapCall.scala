package loamstream.model.calls

import loamstream.model.recipes.{LPileCalls, LRecipe}

import scala.reflect.runtime.universe.{TypeTag, typeTag}

/**
  * LoamStream
  * Created by oliverr on 12/23/2015.
  */
object LMapCall {

  //  def getPreexisting[Tag <: LMapTag[_, _, _], Props <: LProps](tag: Tag, id: String):
  //  LMapCall[Tag, LPileCalls.LCalls0, Props]
  //  = LMapCall(tag, new LCheckoutPreexisting(id))
  def apply[Keys <: LKeys[_, _] : TypeTag, V: TypeTag,
  Inputs <: LPileCalls[_, _]](recipe: LRecipe[Inputs]): LMapCall[Keys, V, Inputs] =
    LMapCall(typeTag[Keys], typeTag[V], recipe)
}

case class LMapCall[Keys <: LKeys[_, _] : TypeTag, V: TypeTag,
Inputs <: LPileCalls[_, _]](keysTag: TypeTag[Keys], vTag: TypeTag[V], recipe: LRecipe[Inputs])
  extends LPileCall[Keys, Inputs] {
}
