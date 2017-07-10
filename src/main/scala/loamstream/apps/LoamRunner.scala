package loamstream.apps

import loamstream.compiler.LoamCompiler
import loamstream.compiler.LoamEngine
import loamstream.compiler.LoamProject
import loamstream.model.jobs.Execution
import loamstream.model.jobs.LJob
import loamstream.util.Hit
import loamstream.util.Loggable
import loamstream.util.Maps
import loamstream.util.Miss
import loamstream.loam.LoamGraph

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
        val chunkSource = compile(project).graphQueue
        
        var jobResults: Map[LJob, Execution] = Map.empty
        
        while(chunkSource.nonEmpty) {
          val chunk = chunkSource.get()

          val chunkGraph = chunk()
          
          val chunkResults = loamEngine.run(chunkGraph)
          
          jobResults ++= chunkResults
        }
        
        // TODO Obviate need for insertion of LoamCompiler.Result
        LoamEngine.Result(Hit(project), Miss(""), Hit(jobResults))
      }
  
      engineResult.jobExecutionsOpt.get
    }
  }
}
