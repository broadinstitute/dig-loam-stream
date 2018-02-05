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
  def run(project: LoamProject): Either[LoamCompiler.Result, Map[LJob, Execution]]
}

object LoamRunner {
  def apply(loamEngine: LoamEngine): LoamRunner = new Default(loamEngine)

  private val emptyJobResults: Map[LJob, Execution] = Map.empty

  private final class Default(loamEngine: LoamEngine) extends LoamRunner with Loggable {

    override def run(project: LoamProject): Either[LoamCompiler.Result, Map[LJob, Execution]] = {

      val compilationResults = loamEngine.compile(project)
      
      info(compilationResults.summary)

      if (compilationResults.isValid && compilationResults.isSuccess) {
        Right(process(compilationResults.graphSource))
      } else {
        Left(compilationResults)
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
          if (chunkGraph.tools.isEmpty) { jobResultsSoFar }
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
