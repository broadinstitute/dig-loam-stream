package loamstream.model.calls

import loamstream.model.recipes.{LPileCalls, LRecipe}
import loamstream.model.tags.LSigTag

import scala.reflect.runtime.universe.{TypeTag, typeTag}

/**
  * LoamStream
  * Created by oliverr on 12/23/2015.
  */
object LMapCall {
  type Map0[V] = LPileCall.Pile0 with LSigTag.HasV[V]
  type Map1[K0, V] = LPileCall.Pile1[K0] with LSigTag.HasV[V]
  type Map2[K0, K1, V] = LPileCall.Pile2[K0, K1] with LSigTag.HasV[V]
  type Map3[K0, K1, K2, V] = LPileCall.Pile3[K0, K1, K2] with LSigTag.HasV[V]
  type Map4[K0, K1, K2, K3, V] = LPileCall.Pile4[K0, K1, K2, K3] with LSigTag.HasV[V]
  type Map5[K0, K1, K2, K3, K4, V] = LPileCall.Pile5[K0, K1, K2, K3, K4] with LSigTag.HasV[V]
  type Map6[K0, K1, K2, K3, K4, K5, V] = LPileCall.Pile6[K0, K1, K2, K3, K4, K5] with LSigTag.HasV[V]
  type Map7[K0, K1, K2, K3, K4, K5, K6, V] = LPileCall.Pile7[K0, K1, K2, K3, K4, K5, K6] with LSigTag.HasV[V]

  //  def getPreexisting[Tag <: LMapTag[_, _, _], Props <: LProps](tag: Tag, id: String):
  //  LMapCall[Tag, LPileCalls.LCalls0, Props]
  //  = LMapCall(tag, new LCheckoutPreexisting(id))
  def apply[SigTag <: LSigTag.IsMap : TypeTag,
  Inputs <: LPileCalls[_, _]](recipe: LRecipe[Inputs]): LMapCall[SigTag, Inputs] =
    LMapCall(typeTag[SigTag], recipe)
}

case class LMapCall[+SigTag <: LSigTag.IsMap : TypeTag,
Inputs <: LPileCalls[_, _]](sigTag: TypeTag[_], recipe: LRecipe[Inputs])
  extends LPileCall[SigTag, Inputs] {
}
