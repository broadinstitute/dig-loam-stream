package loamstream.compiler

import loamstream.pipeline.examples.PipelineTargeted

/**
 * LoamStream
 * Created by oliverr on 10/6/2015.
 */
object CompileApp extends App {

  val pipeline = PipelineTargeted
  println(CamelBricksCompiler.compile(pipeline))

}
