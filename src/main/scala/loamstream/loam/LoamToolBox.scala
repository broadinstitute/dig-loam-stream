package loamstream.loam

import java.net.URI
import java.nio.file.{Path, Paths}
import loamstream.googlecloud.CloudStorageClient
import loamstream.model.execute.Executable
import loamstream.model.execute.Environment
import loamstream.model.jobs.commandline.CommandLineJob
import loamstream.model.jobs.{LJob, NativeJob, Output}
import loamstream.model.{Store, Tool}
import loamstream.loam.ast.AST
import loamstream.util.{Hit, Miss, Shot, Snag}
import loamstream.model.jobs.JobNode

/**
  * LoamStream
  * Created by oliverr on 6/21/2016.
  */
final class LoamToolBox(graph: LoamGraph, client: Option[CloudStorageClient] = None) {

  @volatile private[this] var loamJobs: Map[Tool, JobNode] = Map.empty

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

    val environment: Environment = graph.executionEnvironmentOpt(tool).getOrElse(Environment.Local)

    val shotsForPrecedingTools: Shot[Set[JobNode]] = Shot.sequence(graph.toolsPreceding(tool).map(getLoamJob))

    shotsForPrecedingTools.map { inputJobs =>
      val outputs = outputsFor(tool)

      val toolNameOpt = graph.nameOf(tool)
      
      tool match {
        case cmdTool: LoamCmdTool => {
          CommandLineJob(cmdTool.commandLine, workDir, environment, inputJobs, outputs, nameOpt = toolNameOpt)
        }
        case nativeTool: LoamNativeTool[_] => NativeJob(nativeTool.expBox, inputJobs, outputs, nameOpt = toolNameOpt)
      }
    }
  }

  def getLoamJob(tool: Tool): Shot[JobNode] = lock.synchronized {
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
    val noJobs: Set[JobNode] = Set.empty

    val jobs: Set[JobNode] = ast match {
      case AST.ToolNode(_, tool, deps) => {
        val jobsOption = for {
          //TODO: fail loudly
          job <- getLoamJob(tool).asOpt
          newInputs = deps.map(_.producer).flatMap(createExecutable(_).jobNodes)
          //newJob = if (newInputs == job.inputs) job else job.withInputs(newInputs)
          newJob = job.withInputs(newInputs)
        } yield {
          Set(newJob)
        }

        jobsOption.getOrElse(noJobs)
      }
      case _ => noJobs //TODO: other AST nodes
    }

    Executable(jobs)
  }
}
