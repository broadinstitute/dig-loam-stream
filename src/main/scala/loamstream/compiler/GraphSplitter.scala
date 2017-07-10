package loamstream.compiler

import loamstream.loam.LoamGraph
import loamstream.loam.LoamProjectState

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
    
    projectContext.state match {
      case LoamProjectState(graph, GraphQueue.Empty) => Seq(() => graph)
      case s => s.graphsSoFar.toSeq
    }
  }
}
