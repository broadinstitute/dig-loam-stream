package loamstream.apps

import java.nio.file.Path

import loamstream.conf.{ImputationConfig, UgerConfig}
import loamstream.model.execute.LExecutable
import loamstream.model.execute.ChunkedExecuter
import loamstream.tools.LineCommand
import loamstream.util.Loggable
import loamstream.util.Files
import java.nio.file.Paths
import loamstream.uger.Jobs
import loamstream.uger.Poller
import loamstream.uger.DrmaaClient
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import loamstream.uger.Drmaa1Client
import loamstream.model.jobs.commandline.CommandLineJob
import loamstream.model.jobs.commandline.CommandLineStringJob
import loamstream.model.AST
import loamstream.uger.UgerChunkRunner
import loamstream.model.jobs.commandline.CommandLineBuilderJob
import loamstream.model.jobs.LJob

/**
  * LoamStream
  * Created by kyuksel on 05/02/2016.
  */
object ImputationApp extends DrmaaClientHelpers with Loggable {

  def main(args: Array[String]) {

    val ugerConfig = UgerConfig.fromFile("loamstream.conf").get
    
    final case class TokenizedCommandLine(tokens: Seq[String]) extends LineCommand.CommandLine {
      override def commandLine = tokens.mkString(LineCommand.tokenSep)
    }

    def commandLine(parts: Seq[String]) = TokenizedCommandLine(parts)
    
    def commandLineJob(parts: String*) = CommandLineBuilderJob(commandLine(parts), ugerConfig.ugerWorkDir)
    
    withClient { drmaaClient =>
      val jobGraph: LJob = {
        val abJob = commandLineJob("cp", "/home/unix/cgilbert/shapeit/a.txt", "/home/unix/cgilbert/shapeit/b.txt")
        
        val bcJob = {
          val bc = commandLineJob("cp", "/home/unix/cgilbert/shapeit/b.txt", "/home/unix/cgilbert/shapeit/c.txt")
          
          bc.withInputs(Set(abJob))
        }
        
        bcJob
      }
      
      import scala.concurrent.ExecutionContext.Implicits.global
      
      val chunkRunner = UgerChunkRunner(ugerConfig, drmaaClient)
      
      val executer = ChunkedExecuter(chunkRunner)
      
      val executable = LExecutable(Set(jobGraph))
      
      info(s"Running Job Graph")
      
      val results = executer.execute(executable)
      
      info(s"Run complete; results:")
      
      for {
        (job, result) <- results
      } {
        info(s"Got $result when running $job")
      }
    }
  }
}
