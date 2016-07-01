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

/**
  * LoamStream
  * Created by kyuksel on 05/02/2016.
  */
object ImputationApp extends Loggable {

  // TODO: Replace with command line interface
  def checkIfArgsValid(args: Array[String]): Unit = {
    val configHelpText = "Please provide path to config file in the form: --config <path> or -c <path> " +
      "and specify whether there is a single or bulk job(s) in the form: --bulk <true/false> or -b <true/false>"

    val longConfigOptionName = "--config"
    val shortConfigOptionName = "-c"

    val longBulkOptionName = "--bulk"
    val shortBulkOptionName = "-b"

    def quitWith(msg: String): Unit = {
      error(configHelpText)
      
      System.exit(-1)
    }
    
    if (args.length != 4) {
      quitWith(configHelpText)
    }

    args(0) match {
      case `longConfigOptionName` | `shortConfigOptionName` => ()
      case _ => quitWith(configHelpText)
    }

    args(2) match {
      case `longBulkOptionName` | `shortBulkOptionName` => ()
      case _ => quitWith(configHelpText)
    }
  }

  def getShapeItCmdLineTokens(
      shapeItExecutable: Path, 
      vcf: Path, 
      map: Path, 
      haps: Path, 
      samples: Path,
      log: Path,
      numThreads: Int = 1): Seq[String] = {
    
    Seq(
      shapeItExecutable, 
      "-V", 
      vcf, 
      "-M", 
      map, 
      "-O", 
      haps, 
      samples, 
      "-L", 
      log, 
      "--thread", 
      numThreads).map(_.toString)
  }

  def runShapeItLocally(configFile: Path, inputFile: Path, outputFile: Path): Unit = {
    trace("Attempting to run ShapeIt...")

    val config = ImputationConfig.fromFile(configFile).get
    val shapeItExecutable = config.shapeIt.executable
    val shapeItWorkDir = config.shapeIt.workDir
    val vcf = inputFile
    val map = config.shapeIt.mapFile
    val haps = outputFile
    val samples = Files.tempFile("shapeit-samples")
    val log = config.shapeIt.logFile
    val numThreads = config.shapeIt.numThreads

    val shapeItTokens = getShapeItCmdLineTokens(shapeItExecutable, vcf, map, haps, samples, log, numThreads)
    val commandLine = shapeItTokens.mkString(LineCommand.tokenSep)
    val shapeItJob = CommandLineStringJob(commandLine, shapeItWorkDir)

    val executable = LExecutable(Set(shapeItJob))
    
    val executer = ChunkedExecuter.default
    
    executer.execute(executable)
  }

  def runShapeItOnUger(drmaaClient: DrmaaClient, configFile: Path): DrmaaClient.SubmissionResult = {
    trace("Attempting to run ShapeIt...")

    //TODO: Fragile
    val shapeItConfig = ImputationConfig.fromFile(configFile).get
    
    val shapeItScript = shapeItConfig.shapeIt.script

    //TODO: Fragile
    val ugerConfig = UgerConfig.fromFile(configFile).get 
    
    val ugerLog = ugerConfig.ugerLogFile

    drmaaClient.submitJob(shapeItScript, ugerLog, "ShapeIt")
  }

  private def withClient[A](f: DrmaaClient => A): A = {
    val drmaaClient = DrmaaClient.drmaa1(new Drmaa1Client)
    
    try { f(drmaaClient) } 
    finally { drmaaClient.shutdown() }
  }
  
  def main(args: Array[String]) {
    def path(s: String) = Paths.get(s)
    
    /*runShapeItLocally(
      path("src/main/resources/loamstream.conf"), 
      path("src/test/resources/imputation/gwas.vcf.gz"),
      path("target/foo"))*/
      
    //runShapeItLocally(args)
    //runShapeItOnUger(path("src/main/resources/loamstream.conf"), true)
    
    withClient { drmaaClient =>
      val result = runShapeItOnUger(drmaaClient, path(args(0)))
    
      if(result.isFailure) {
        Console.err.println(s"Failure: $result")
      } else {
        val DrmaaClient.BulkJobSubmissionResult(jobIds) = result
      
        if(jobIds.tail.nonEmpty) {
          Console.err.println(s"Expected 1 job but got $jobIds")
        
          System.exit(1)
        }
      
        val jobId = jobIds.head.toString
      
        //import scala.concurrent.ExecutionContext.Implicits.global
        import monix.execution.Scheduler.Implicits.global
        
        val poller = Poller.drmaa1(drmaaClient)
      
        val fut = Jobs.monitor(poller, 0.2)(jobId).foreach { jobStatus => 
          println(s"Job '$jobId': $jobStatus")
        }
      
        Await.result(fut, Duration.Inf)
      
        //Block until a keypress
        println("Press any key to exit")
        
        Console.in.readLine()
      }
    }
  }
}
