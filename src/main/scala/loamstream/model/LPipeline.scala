package loamstream.model

/**
 * LoamStream
 * Created by oliverr on 2/17/2016.
 */
trait LPipeline {
  def stores: Set[Store]

  def tools: Set[Tool]
}

object LPipeline {

  final case class Flat(stores: Set[Store], tools: Set[Tool]) extends LPipeline

}

