package loamstream.model

/**
  * LoamStream
  * Created by oliverr on 2/17/2016.
  */
trait LPipeline {
  def stores: Set[StoreSpec]

  def tools: Set[Tool]
}
