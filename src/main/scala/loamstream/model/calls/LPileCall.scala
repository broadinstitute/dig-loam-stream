package loamstream.model.calls

import loamstream.model.recipes.{LPileCalls, LRecipe}
import loamstream.model.tags.{LSemTag, LSigTag}
import util.Index.{I00, I01, I02, I03, I04, I05, I06}

import scala.reflect.runtime.universe.TypeTag

/**
  * LoamStream
  * Created by oliverr on 12/23/2015.
  */
object LPileCall {
  type Pile0 = LSigTag
  type Pile1[K0] = LSigTag.HasKey[I00, K0]
  type Pile2[K0, K1] = Pile1[K0] with LSigTag.HasKey[I01, K1]
  type Pile3[K0, K1, K2] = Pile2[K0, K1] with LSigTag.HasKey[I02, K2]
  type Pile4[K0, K1, K2, K3] = Pile3[K0, K1, K2] with LSigTag.HasKey[I03, K3]
  type Pile5[K0, K1, K2, K3, K4] = Pile4[K0, K1, K2, K3] with LSigTag.HasKey[I04, K4]
  type Pile6[K0, K1, K2, K3, K4, K5] = Pile5[K0, K1, K2, K3, K4] with LSigTag.HasKey[I05, K5]
  type Pile7[K0, K1, K2, K3, K4, K5, K6] = Pile6[K0, K1, K2, K3, K4, K5] with LSigTag.HasKey[I06, K6]
}

trait LPileCall[+SigTag <: LSigTag, +SemTag <: LSemTag, Inputs <: LPileCalls[_, _]] {
  def sigTag: TypeTag[_]

  def semTag: TypeTag[_]

  def recipe: LRecipe[Inputs]
}
