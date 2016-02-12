package loamstream.model.calls

import loamstream.model.kinds.LKind
import loamstream.model.recipes.{LCheckoutPreexisting, LRecipe}

/**
  * LoamStream
  * Created by oliverr on 12/23/2015.
  */
object LPileCall {
  def getPreexisting(sig: LSig, kind: LKind, id: String) = LPileCall(sig, kind, new LCheckoutPreexisting(id))
}

case class LPileCall(sig: LSig, kind: LKind, recipe: LRecipe) {
  def extractKey(index: Int, kind: LKind) =
    LPileCall(LSig.Set(Seq(sig.keyTypes(index))), kind, LRecipe.ExtractKey(this, index))
}
