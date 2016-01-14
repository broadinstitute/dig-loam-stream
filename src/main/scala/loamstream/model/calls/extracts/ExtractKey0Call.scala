package loamstream.model.calls.extracts

import loamstream.model.calls.{LPileCall, LSetCall}
import loamstream.model.calls.extracts.ExtractKey0Call.Recipe
import loamstream.model.recipes.{LRecipe, LPileCalls}
import loamstream.model.tags.{LSemTag, LSigTag}
import util.Index.I00
import scala.reflect.runtime.universe.{TypeTag, typeTag}

/**
 * RugLoom - A prototype for a pipeline building toolkit
 * Created by oruebenacker on 1/14/16.
 */
object ExtractKey0Call {

  case class Recipe[K0, Input <: LPileCall[LSigTag.HasKey[I00, K0], _, _]](input: Input)
    extends LRecipe[LPileCalls.LCalls1[Input]] {
    val inputs = LPileCalls.calls1(input)
  }

  def apply[K0, SigTag <: LSigTag.HasKey[I00, K0] : TypeTag,
  Input <: LPileCall[SigTag, _, _]](input: Input) = new ExtractKey0Call[K0, SigTag, Input](input)

}

class ExtractKey0Call[K0, SigTag <: LSigTag.HasKey[I00, K0] : TypeTag,
Input <: LPileCall[SigTag, _, _]](input: Input)
  extends LSetCall[LSigTag.HasKey[I00, K0], LSemTag,
    LPileCalls.LCalls1[Input]](typeTag[SigTag], typeTag[LSemTag], Recipe(input)) {

}
