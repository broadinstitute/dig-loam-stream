package loamstream.model

/**
  * LoamStream
  * Created by oliverr on 2/17/2016.
  */
object LPipeline {

  case class Flat(stores: Set[Store], tools: Set[Tool]) extends LPipeline

}

trait LPipeline {
  def stores: Set[Store]

  def tools: Set[Tool]
}
