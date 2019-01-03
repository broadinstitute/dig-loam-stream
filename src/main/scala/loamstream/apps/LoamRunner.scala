package loamstream.apps

import loamstream.compiler.LoamCompiler
import loamstream.compiler.LoamEngine
import loamstream.compiler.LoamProject
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

      compilationResults match {
        case success: LoamCompiler.Result.Success => Right(loamEngine.run(success.graph))
        case _ => Left(compilationResults)
      }
    }
  }
}
