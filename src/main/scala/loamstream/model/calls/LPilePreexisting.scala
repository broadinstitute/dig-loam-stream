package loamstream.model.calls

import loamstream.model.calls.props.LProps
import loamstream.model.recipes.{LCheckoutPreexisting, LPileCalls}
import loamstream.model.tags.LPileTag

/**
  * LoamStream
  * Created by oliverr on 1/7/2016.
  */
trait LPilePreexisting[Tag <: LPileTag[_, _], +Props <: LProps] extends LPileCall[Tag, LPileCalls.LCalls0, Props] {
  def id: String

  val recipe = new LCheckoutPreexisting(id)

}
