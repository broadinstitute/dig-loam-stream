package loamstream.loam

import java.net.URI
import java.nio.file.{Path, Paths}

import loamstream.googlecloud.CloudStorageClient
import loamstream.loam.ops.filters.LoamStoreFilterTool
import loamstream.loam.ops.mappers.LoamStoreMapperTool
import loamstream.model.execute.{Executable, ExecutionEnvironment}
import loamstream.model.jobs.commandline.CommandLineStringJob
import loamstream.model.jobs.ops.{StoreFilterJob, StoreMapperJob}
import loamstream.model.jobs.{LJob, NativeJob, Output}
import loamstream.model.{AST, Store, Tool}
import loamstream.util.{Hit, Miss, Shot, Snag}

/**
  * LoamStream
  * Created by oliverr on 6/21/2016.
  */
final class LoamToolBox(context: LoamProjectContext, client: Option[CloudStorageClient] = None) {

  @volatile private[this] var loamJobs: Map[LoamTool, LJob] = Map.empty

  private[this] val lock = new AnyRef

  private[loam] def newLoamJob(tool: LoamTool): Shot[LJob] = {
    val graph = tool.graphBox.value

    def outputsFor(tool: LoamTool): Set[Output] = {
      val loamStores: Set[Store.Untyped] = graph.toolOutputs(tool)

      def pathOrUriToOutput(store: Store.Untyped): Option[Output] = {
        store.pathOpt.orElse(store.uriOpt).map {
          case path: Path => Output.PathOutput(path)
          case uri: URI => Output.GcsUriOutput(uri, client)
        }
      }

      loamStores.flatMap(pathOrUriToOutput(_))
    }

    val workDir: Path = graph.workDirOpt(tool).getOrElse(Paths.get("."))

    val environment: ExecutionEnvironment = graph.executionEnvironmentOpt(tool).getOrElse(ExecutionEnvironment.Local)

    val shotsForPrecedingTools: Shot[Set[LJob]] = Shot.sequence(graph.toolsPreceding(tool).map(getLoamJob))

    shotsForPrecedingTools.map { inputJobs =>
      val outputs = outputsFor(tool)

      tool match {
        case cmdTool: LoamCmdTool =>
          CommandLineStringJob(cmdTool.commandLine, workDir, environment, inputJobs, outputs)
        case storeFilterTool: LoamStoreFilterTool[_] =>
          val inStore = graph.toolInputs(tool).head
          val outStore = graph.toolOutputs(tool).head
          StoreFilterJob(inStore.path, outStore.path, inStore.sig.tpe, inputJobs, outputs, storeFilterTool.filter)
        case storeMapperTool: LoamStoreMapperTool[_, _] =>
          val inStore = graph.toolInputs(tool).head
          val outStore = graph.toolOutputs(tool).head
          StoreMapperJob(inStore.path, outStore.path, inStore.sig.tpe, outStore.sig.tpe, inputJobs, outputs,
            storeMapperTool.mapper)
        case nativeTool: LoamNativeTool[_] => NativeJob(nativeTool.expBox, inputJobs, outputs)
      }
    }
  }

  private[loam] def getLoamJob(tool: LoamTool): Shot[LJob] = lock.synchronized {
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

  def toolToJobShot(tool: Tool): Shot[LJob] = tool match {
    case loamTool: LoamTool => getLoamJob(loamTool)
    case _ => Miss(Snag(s"LoamToolBox only knows Loam tools; it doesn't know about $tool."))
  }
}
