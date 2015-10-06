package loamstream.compiler

/**
 * LoamStream
 * Created by oliverr on 10/6/2015.
 */
object CompileApp extends App {

  val pipeline = PipelineMta
  println(Compiler.compile(pipeline))

}
