package loamstream.model

/**
  * LoamStream
  * Created by oliverr on 2/17/2016.
  */
object LPipeline {

  case class Flat(piles: Set[LPile], recipes: Set[LRecipe]) extends LPipeline

}

trait LPipeline {
  def stores: Set[Store]

  def tools: Set[Tool]
}
