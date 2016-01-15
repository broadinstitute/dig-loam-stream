package loamstream.model.calls

import loamstream.model.recipes.{LCheckoutPreexisting, LPileCalls, LRecipe}
import loamstream.model.tags.LSigTag

import scala.reflect.runtime.universe.{TypeTag, typeTag}

/**
  * LoamStream
  * Created by oliverr on 12/23/2015.
  */
object LSetCall {
  type Set0 = LPileCall.Pile0 with LSigTag.IsSet
  type Set1[K0] = LPileCall.Pile1[K0] with LSigTag.IsSet
  type Set2[K0, K1] = LPileCall.Pile2[K0, K1] with LSigTag.IsSet
  type Set3[K0, K1, K2] = LPileCall.Pile3[K0, K1, K2] with LSigTag.IsSet
  type Set4[K0, K1, K2, K3] = LPileCall.Pile4[K0, K1, K2, K3] with LSigTag.IsSet
  type Set5[K0, K1, K2, K3, K4] = LPileCall.Pile5[K0, K1, K2, K3, K4] with LSigTag.IsSet
  type Set6[K0, K1, K2, K3, K4, K5] = LPileCall.Pile6[K0, K1, K2, K3, K4, K5] with LSigTag.IsSet
  type Set7[K0, K1, K2, K3, K4, K5, K6] = LPileCall.Pile7[K0, K1, K2, K3, K4, K5, K6] with LSigTag.IsSet

  def getPreexisting[SigTag <: LSigTag.IsSet](id: String):
  LSetCall[SigTag, LPileCalls.LCalls0]
  = apply(new LCheckoutPreexisting(id))

  def apply[SigTag <: LSigTag.IsSet : TypeTag,
  Inputs <: LPileCalls[_, _]](recipe: LRecipe[Inputs]): LSetCall[SigTag, Inputs] =
    LSetCall(typeTag[SigTag], recipe)
}

case class LSetCall[+SigTag <: LSigTag.IsSet : TypeTag,
Inputs <: LPileCalls[_, _]](sigTag: TypeTag[_], recipe: LRecipe[Inputs])
  extends LPileCall[SigTag, Inputs] {
}
