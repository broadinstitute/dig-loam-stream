package loamstream.apps

import scala.util.control.NonFatal

import loamstream.compiler.GraphSource
import loamstream.compiler.GraphThunk
import loamstream.compiler.LoamCompiler
import loamstream.compiler.LoamEngine
import loamstream.compiler.LoamProject
import loamstream.loam.LoamGraph
import loamstream.model.Tool
import loamstream.model.jobs.Execution
import loamstream.model.jobs.LJob
import loamstream.util.Loggable

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
        else {
          compilationResults.errors.foreach(e => error(s"Compilation error: $e"))
          
          emptyJobResults 
        }
      }
    }

    private def process(graphSource: GraphSource): Map[LJob, Execution] = {
      //Keep track of job-execution results and the tools we've run "so far"
      val z: (Map[LJob, Execution], Set[Tool]) = (emptyJobResults, Set.empty)
      
      debug("Processing graph chunks")
      
      //Fold over the "stream" of graph chunks, producing job-execution results
      val (jobResults, _) = graphSource.iterator.foldLeft(z) { (state, chunk) =>
        
        val (jobResultsSoFar, toolsRunSoFar) = state
        
        val rawChunkGraph = getGraphFrom(chunk)
        
        debug(s"Made raw chunk graph with ${rawChunkGraph.tools.size} tools and ${rawChunkGraph.stores.size} stores.")
        
        //Filter out tools from previous chunks, so we don't run jobs more than necessary, saving
        //the expense of calculating if a job can be skipped.
        val chunkGraph = rawChunkGraph.without(toolsRunSoFar)

        debug(s"Made filtered chunk graph with ${chunkGraph.tools.size} tools and ${chunkGraph.stores.size} stores.")
        
        //Skip running if there are no new tools
        val jobResults = {
          if(chunkGraph.tools.isEmpty) { jobResultsSoFar }
          else { jobResultsSoFar ++ loamEngine.run(chunkGraph) }
        }
        
        (jobResults, toolsRunSoFar ++ chunkGraph.tools)
      }
      
      jobResults
    }
    
    private def getGraphFrom(thunk: GraphThunk): LoamGraph = {
      try { thunk() }
      catch {
        case NonFatal(e) => {
          error(s"Evaluating loam code chunk threw exception: $e", e)
          
          throw e
        }
      }
    }
  }
}
