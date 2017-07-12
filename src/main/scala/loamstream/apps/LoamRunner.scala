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
import loamstream.model.jobs.JobResult
import loamstream.compiler.GraphQueue
import loamstream.compiler.GraphSource

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
    shutdownAfter: (=> Map[LJob, Execution]) => Map[LJob, Execution]): LoamRunner = {

    new Default(loamEngine, compile, shutdownAfter)
  }

  private val emptyJobResults: Map[LJob, Execution] = Map.empty

  final class Default(
      loamEngine: LoamEngine,
      compile: LoamProject => LoamCompiler.Result,
      shutdownAfter: (=> Map[LJob, Execution]) => Map[LJob, Execution]) extends LoamRunner with Loggable {

    override def run(project: LoamProject): Map[LJob, Execution] = {
      //NB: Shut down before logging anything about jobs, so that potentially-noisy shutdown info is logged
      //before final job statuses.
      shutdownAfter {
        val compilationResults = compile(project)

        if (compilationResults.isValid) { process(compilationResults.graphSource) }
        else { emptyJobResults }
      }
    }

    private def process(graphSource: GraphSource): Map[LJob, Execution] = {
      val z: (Map[LJob, Execution], LoamGraph) = (emptyJobResults, LoamGraph.empty)
      
      val (jobResults, _) = graphSource.iterator.foldLeft(z) { (state, chunk) =>
        val (jobResultsAcc, lastGraph) = state
        
        val chunkGraph = chunk().without(lastGraph.tools)

        val jobResults = jobResultsAcc ++ loamEngine.run(chunkGraph)
        
        (jobResults, chunkGraph)
      }
      
      jobResults
    }
  }
}
