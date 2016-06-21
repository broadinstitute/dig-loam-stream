package loamstream.loam

import java.nio.file.Path

import loamstream.LEnv
import loamstream.model.execute.LExecutable
import loamstream.model.jobs.commandline.CommandLineStringJob
import loamstream.model.jobs.{LJob, LToolBox}
import loamstream.model.{AST, LPipeline, Tool}
import loamstream.util.{Hit, Shot}

/**
  * LoamStream
  * Created by oliverr on 6/21/2016.
  */
case class LoamToolBox(env: LEnv) extends LToolBox {

  def loamToolJob(tool: LoamTool): Shot[LJob] = {
    tool.graphBuilder.applyEnv(env)
    val graph = tool.graphBuilder.graph
    val commandLineString = graph.toolTokens(tool).mkString
    val workDir: Path = ??? // TODO
    val inputs: Set[LJob] = ??? // TODO
    Hit(CommandLineStringJob(commandLineString, workDir, inputs))
  }

  @deprecated("", "")
  override def createJobs(tool: Tool, pipeline: LPipeline): Shot[Set[LJob]] = ???

  @deprecated("", "")
  override def createExecutable(pipeline: LPipeline): LExecutable = ???

  override def createExecutable(ast: AST): LExecutable = ???
}
