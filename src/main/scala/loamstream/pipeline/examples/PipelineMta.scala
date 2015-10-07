package loamstream.pipeline.examples

import loamstream.pipeline.Pipeline

/**
 * LoamStream
 * Created by oliverr on 10/6/2015.
 */
object PipelineMta extends Pipeline {
  override def asString: String = PipelineMtaPart1.string + PipelineMtaPart2.string + PipelineMtaPart3.string

}

