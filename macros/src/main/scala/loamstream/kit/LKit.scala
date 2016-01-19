package loamstream.kit

import loamstream.model.calls.LPileCall
import loamstream.model.recipes.LPileCalls
import util.Index
import scala.language.experimental.macros

/**
  * LoamStream
  * Created by oliverr on 1/19/2016.
  */
object LKit {

  class KeyOps[I <: Index] {
    def extractKey[I <: Index, Keys <: Product,
    Inputs <: LPileCalls[_, _]](pile: LPileCall[Keys, Inputs]) = macro LKitMacrosImpl.extractKey[I, Keys, Inputs]
  }

}
