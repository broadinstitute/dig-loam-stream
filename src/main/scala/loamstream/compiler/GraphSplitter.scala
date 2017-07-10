package loamstream.compiler

import loamstream.loam.LoamGraph

/**
 * @author clint
 * Jul 5, 2017
 */
final class GraphSplitter(compiler: LoamCompiler) {
  def chunks(project: LoamProject): Seq[() => LoamGraph] = {
    val result = compiler.compile(project)
    
    //TODO
    if(!result.isValid) {
      result.errors.foreach(println)
    }
    
    require(result.isValid, "Compilation failed")
    
    val projectContext = result.contextOpt.get
    
    projectContext.graphsSoFar.get {
      case Nil => Seq(() => projectContext.graph)
      case thunks => thunks
    }
  }
}
