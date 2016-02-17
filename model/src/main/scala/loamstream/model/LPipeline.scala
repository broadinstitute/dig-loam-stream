package loamstream.model

import loamstream.model.calls.LPileCall

/**
  * LoamStream
  * Created by oliverr on 2/17/2016.
  */
object LPipeline {
  def apply(calls: LPileCall*): LPipeline = LPipeline(calls.toSet)
}

case class LPipeline(calls: Set[LPileCall]) {

}
