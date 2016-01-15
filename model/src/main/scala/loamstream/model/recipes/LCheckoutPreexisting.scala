package loamstream.model.recipes

/**
  * LoamStream
  * Created by oliverr on 1/5/2016.
  */
case class LCheckoutPreexisting(id: String) extends LRecipe[LPileCalls.LCalls0] {
  override def inputs = LPileCallsNil
}
