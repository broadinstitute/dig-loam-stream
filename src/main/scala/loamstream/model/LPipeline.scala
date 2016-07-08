package loamstream.model

/**
 * LoamStream
 * Created by oliverr on 2/17/2016.
 */
@deprecated("", "")
trait LPipeline {
  def stores: Set[Store]

  def tools: Set[Tool]
}
