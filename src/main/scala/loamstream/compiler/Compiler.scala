package loamstream.compiler

import loamstream.pipeline.Pipeline

/**
 * LoamStream
 * Created by oliverr on 10/6/2015.
 */
object Compiler {

  def compile(pipeline: Pipeline): String = pipeline.asString

}
