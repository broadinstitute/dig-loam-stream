package loamstream.compiler

/**
 * LoamStream
 * Created by oliverr on 10/6/2015.
 */
object Compiler {

  def compile(pipeline: Pipeline): String = pipeline.asString

}
