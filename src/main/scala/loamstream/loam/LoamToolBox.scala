package loamstream.loam

import java.net.URI
import java.nio.file.{Path, Paths}

import loamstream.googlecloud.CloudStorageClient
import loamstream.model.execute.{Executable, ExecutionEnvironment}
import loamstream.model.jobs.commandline.CommandLineStringJob
import loamstream.model.jobs.{LJob, NativeJob, Output}
import loamstream.model.{AST, Store, Tool}
import loamstream.util.{Hit, Miss, Shot, Snag}

/**
  * LoamStream
  * Created by oliverr on 6/21/2016.
  */
final class LoamToolBox(graph: LoamGraph, client: Option[CloudStorageClient] = None) {

  @volatile private[this] var loamJobs: Map[Tool, LJob] = Map.empty

  private[this] val lock = new AnyRef

  private[loam] def newLoamJob(tool: Tool): Shot[LJob] = {
    def outputsFor(tool: Tool): Set[Output] = {
      val loamStores: Set[Store] = graph.toolOutputs(tool)

      def pathOrUriToOutput(store: Store): Option[Output] = {
        store.pathOpt.orElse(store.uriOpt).map {
          case path: Path => Output.PathOutput(path)
          case uri: URI => Output.GcsUriOutput(uri, client)
        }
      }

      loamStores.flatMap(pathOrUriToOutput)
    }

    val workDir: Path = graph.workDirOpt(tool).getOrElse(Paths.get("."))

    val environment: ExecutionEnvironment = graph.executionEnvironmentOpt(tool).getOrElse(ExecutionEnvironment.Local)

    val shotsForPrecedingTools: Shot[Set[LJob]] = Shot.sequence(graph.toolsPreceding(tool).map(getLoamJob))

    shotsForPrecedingTools.map { inputJobs =>
      val outputs = outputsFor(tool)

      tool match {
        case cmdTool: LoamCmdTool => {
          CommandLineStringJob(cmdTool.commandLine, workDir, environment, inputJobs, outputs)
        }
        case nativeTool: LoamNativeTool[_] => NativeJob(nativeTool.expBox, inputJobs, outputs)
      }
    }
  }

  private[loam] def getLoamJob(tool: Tool): Shot[LJob] = lock.synchronized {
    loamJobs.get(tool) match {
      case Some(job) => Hit(job)
      case _ => newLoamJob(tool) match {
        case jobHit@Hit(job) =>
          loamJobs += tool -> job
          jobHit
        case miss: Miss => miss
      }
    }
  }

  def createExecutable(ast: AST): Executable = {
    val noJobs: Set[LJob] = Set.empty

    val jobs: Set[LJob] = ast match {
      case AST.ToolNode(_, tool, deps) =>
        val jobsOption = for {
        //TODO: Don't convert to option, pass misses through and fail loudly
          job <- toolToJobShot(tool).asOpt
          newInputs = deps.map(_.producer).flatMap(createExecutable(_).jobs)
          newJob = if (newInputs == job.inputs) job else job.withInputs(newInputs)
        } yield {
          Set[LJob](newJob)
        }

        jobsOption.getOrElse(noJobs)
      case _ => noJobs //TODO: other AST nodes
    }

    Executable(jobs)
  }

  def toolToJobShot(tool: Tool): Shot[LJob] = getLoamJob(tool)
}
