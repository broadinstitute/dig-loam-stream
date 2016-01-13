package loamstream.model.calls

import loamstream.model.calls.props.LProps
import loamstream.model.recipes.{LPileCalls, LRecipe}
import loamstream.model.tags.LTags
import util.Index.{I00, I01, I02, I03, I04, I05, I06}

/**
  * LoamStream
  * Created by oliverr on 12/23/2015.
  */
object LPileCall {
  type Pile1[K0] = LTags.HasKey[I00, K0]
  type Pile2[K0, K1] = Pile1[K0] with LTags.HasKey[I01, K1]
  type Pile3[K0, K1, K2] = Pile2[K0, K1] with LTags.HasKey[I02, K2]
  type Pile4[K0, K1, K2, K3] = Pile3[K0, K1, K2] with LTags.HasKey[I03, K3]
  type Pile5[K0, K1, K2, K3, K4] = Pile4[K0, K1, K2, K3] with LTags.HasKey[I04, K4]
  type Pile6[K0, K1, K2, K3, K4, K5] = Pile5[K0, K1, K2, K3, K4] with LTags.HasKey[I05, K5]
  type Pile7[K0, K1, K2, K3, K4, K5, K6] = Pile6[K0, K1, K2, K3, K4, K5] with LTags.HasKey[I06, K6]
}

trait LPileCall[+Tags <: LTags, Inputs <: LPileCalls[_, _], +Props <: LProps] {
  def recipe: LRecipe[Inputs]

  def withTags[TagsNew <: LTags]: LPileCall[Tags with TagsNew, Inputs, Props]

  def withProps[PropsNew <: LProps]: LPileCall[Tags, Inputs, Props with PropsNew]
}
