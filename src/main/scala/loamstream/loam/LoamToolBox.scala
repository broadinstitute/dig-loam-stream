package loamstream.loam

import java.nio.file.{Path, Paths}

import loamstream.LEnv
import loamstream.model.execute.LExecutable
import loamstream.model.jobs.commandline.CommandLineStringJob
import loamstream.model.jobs.{LJob, LToolBox}
import loamstream.model.{AST, LPipeline, Tool}
import loamstream.util.{Hit, Miss, Shot, Shots}

/**
  * LoamStream
  * Created by oliverr on 6/21/2016.
  */
case class LoamToolBox(env: LEnv) extends LToolBox {

  var loamJobs: Map[LoamTool, LJob] = Map.empty

  def newLoamJob(tool: LoamTool): Shot[LJob] = {
    tool.graphBuilder.applyEnv(env)
    val graph = tool.graphBuilder.graph
    val commandLineString = graph.toolTokens(tool).mkString
    val workDir: Path = graph.workDirOpt(tool).getOrElse(Paths.get("."))
    Shots.unpack(graph.toolsPreceding(tool).map(getLoamJob)).map(inputJobs =>
      CommandLineStringJob(commandLineString, workDir, inputJobs.toSet)
    )
  }

  def getLoamJob(tool: LoamTool): Shot[LJob] = loamJobs.get(tool) match {
    case Some(job) => Hit(job)
    case _ =>
      newLoamJob(tool) match {
        case jobHit@Hit(job) =>
          loamJobs += tool -> job
          jobHit
        case miss: Miss => miss
      }
  }

  @deprecated("", "")
  override def createJobs(tool: Tool, pipeline: LPipeline): Shot[Set[LJob]] = ???

  @deprecated("", "")
  override def createExecutable(pipeline: LPipeline): LExecutable = ???

  override def createExecutable(ast: AST): LExecutable = ???
}
