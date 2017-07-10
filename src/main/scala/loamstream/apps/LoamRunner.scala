package loamstream.apps

import loamstream.compiler.GraphSplitter
import loamstream.compiler.LoamCompiler
import loamstream.compiler.LoamEngine
import loamstream.compiler.LoamProject
import loamstream.model.jobs.Execution
import loamstream.model.jobs.LJob
import loamstream.util.Hit
import loamstream.util.Loggable
import loamstream.util.Maps
import loamstream.util.Miss

/**
 * @author clint
 * @author kyuksel
 * Jun 26, 2017
 */
trait LoamRunner {
  def run(project: LoamProject): Map[LJob, Execution]
}

object LoamRunner {
  def apply(
      loamEngine: LoamEngine, 
      compile: LoamProject => LoamCompiler.Result,
      shutdownAfter: (=> LoamEngine.Result) => LoamEngine.Result): LoamRunner = {

    new Default(loamEngine, compile, shutdownAfter)
  }
  
  final class Default(
      loamEngine: LoamEngine, 
      compile: LoamProject => LoamCompiler.Result,
      shutdownAfter: (=> LoamEngine.Result) => LoamEngine.Result) extends LoamRunner with Loggable {
    
    override def run(project: LoamProject): Map[LJob, Execution] = {
      
      //NB: Shut down before logging anything about jobs, so that potentially-noisy shutdown info is logged
      //before final job statuses.
      val engineResult = shutdownAfter {
        val splitter = new GraphSplitter(loamEngine.compiler)
        
        val chunks = splitter.chunks(project)
        
        val chunkResults = for {
          chunk <- chunks
        } yield {
          val chunkGraph = chunk()
          
          loamEngine.run(chunkGraph)
        }
        
        val jobResults = Maps.mergeMaps(chunkResults)
        
        //val jobResults = loop(project, Map.empty)
        // TODO Obviate need for insertion of LoamCompiler.Result
        LoamEngine.Result(Hit(project), Miss(""), Hit(jobResults))
      }
  
      engineResult.jobExecutionsOpt.get
    }
  }
}
