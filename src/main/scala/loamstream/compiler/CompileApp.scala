package loamstream.compiler

/**
 * LoamStream
 * Created by oliverr on 10/6/2015.
 */
object CompileApp extends App {

  val pipeline = PipelineTargeted
  println(Compiler.compile(pipeline))

}
