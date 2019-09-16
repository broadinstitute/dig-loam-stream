package loamstream

import java.nio.file.Path

import scala.util.Success
import scala.util.Try

import org.scalatest.FunSuite

import IntegrationTestHelpers.path
import loamstream.util.BashScript
import loamstream.util.ExitCodes
import loamstream.util.Files
import loamstream.util.LogContext
import loamstream.util.Loggable
import loamstream.util.Paths
import loamstream.util.Paths.Implicits.PathHelpers
import loamstream.util.ProcessLoggers
import loamstream.util.Processes
import loamstream.util.RunResults
import loamstream.util.Tries
import scala.concurrent.duration.Duration

/**
 * @author clint
 * Jun 12, 2019
 * 
 * Runs a simple pipeline at EBI.  The pipeline consists of 2 copy commands, one run in a minimal Singularity
 * container and one not.
 * 
 * NB: Assumes EBI-specific values (particularly for host `mitigate` are in `diguser`'s `~/.ssh/config`.
 * NB: Relies on new SSH key made according to EBI's instructions and authorized on the EBI side, stored in 
 * `/humgen/diabetes/users/dig/loamstream/ci/jenkins/secrets/ebi-tests/ssh/`.
 * 
 * Outline of steps:
 * - Deletes and re-creates a remote work dir
 * - Copies a LS jar to EBI (assumes `assembly` has been run first)
 * - Copies a minimal Singularity image to EBI
 * - Copies needed input, conf, and loam files to EBI
 * - Runs LS at EBI
 * - Copies outputs back over from EBI
 * - Validates outputs
 * 
 * NB: the above steps run as the user `cgilbert`.  This has some downsides, but was expedient and in line 
 * with EBI's policies.
 */
final class LsAtEbiTest extends FunSuite {
  import LsAtEbiTest._
  
  // NB: Disabled until SSH key authorization situation is sorted out.
  // -Clint Sep 10, 2019
  ignore("Run a simple pipeline at EBI with some jobs in Singularity containers and some not") {
    val remoteWorkDir = remoteBaseDir / "simple-mixed-pipeline"
    
    val localWorkDir = IntegrationTestHelpers.getWorkDirUnderTarget()
    
    import scala.concurrent.duration._
    
    def sleep(duration: Duration): Try[Unit] = Try(Thread.sleep(duration.toMillis))
    
    val outputFileAttempt = for {
      _ <- runAtEbi(s"rm -rf ${remoteWorkDir}")
      _ <- sleep(1.second)
      _ <- runAtEbi(s"mkdir -p ${remoteWorkDir}")
      _ <- sleep(1.second)
      _ <- copyLsJar(remoteWorkDir)
      _ <- sleep(1.second)
      _ <- copySimgFile(remoteWorkDir)
      _ <- sleep(1.second)
      _ <- copyLoamFile(remoteWorkDir)
      _ <- sleep(1.second)
      _ <- copyConfFile(remoteWorkDir)
      _ <- sleep(1.second)
      _ <- copyInputFile(remoteWorkDir)
      _ <- sleep(1.second)
      _ <- runLoamStream(remoteWorkDir)
      outputFile <- copyOutputFile(remoteWorkDir / "C.txt", localWorkDir / "output.txt")
    } yield outputFile

    val outputFile = outputFileAttempt.get
    
    val expectedContents: String = "ASDF"
    
    assert(Files.readFrom(outputFile).trim === expectedContents)
  }
}

object LsAtEbiTest extends Loggable {
  private val remoteBaseDir: Path = path("loamstream-integration-tests")
  
  private val ebiUser = "cgilbert"
  private val ebiHost = "ebi-cli"
  private val ebiUserAtHost = s"${ebiUser}@${ebiHost}"
  
  private val localResourceDir = path("src/it/resources/ls-at-ebi/")
  
  private val remoteLsJarFilename = "loamstream.jar"
  private val remoteLoamFilename = "test.loam"
  private val remoteSimgFilename = "bash-4.4.simg"
  private val remoteLoamstreamConfFilename = "loamstream.conf"
  
  private val ebiScp = localResourceDir / "bin" / "ebi-scp"
  private val ebiCli = localResourceDir / "bin" / "ebi-cli"
  
  private def copyLsJar(remoteDest: Path): Try[Unit] = {
    //NB: Assumes that `assembly` has been run before `it:test` or `it:testOnly`  
    copyToEbi(path("target/scala-2.12/loamstream-assembly-1.4-SNAPSHOT.jar"), remoteDest / remoteLsJarFilename) 
  }
  
  private def copyLoamFile(remoteDest: Path): Try[Unit] = {
    copyToEbi(localResourceDir / "test.loam", remoteDest / remoteLoamFilename)
  }
  
  private def copySimgFile(remoteDest: Path): Try[Unit] = {
    copyToEbi(localResourceDir / "bash-4.4.simg", remoteDest / remoteSimgFilename)
  }
  
  private def copyConfFile(remoteWorkDir: Path): Try[Unit] = {
    copyToEbi(localResourceDir / "loamstream.conf", remoteWorkDir / remoteLoamstreamConfFilename)
  }
  
  private def toUnit[A](ignored: A): Unit = ()
  
  private def doToFailureIfNeeded(msg: String)(runResults: RunResults): Try[Unit] = {
    if(ExitCodes.isSuccess(runResults.exitCode)) { Success(()) }
    else { 
      val message = s"${msg} (exit code ${runResults.exitCode}) " +
      s"stderr follows: '${runResults.stderr.mkString(System.lineSeparator)}'"
        
      Tries.failure(message) 
    }
  }
  
  private def copyToEbi(localFile: Path, remoteDest: Path): Try[Unit] = {
    val remoteHostAndPath = s"${ebiUserAtHost}:${remoteDest}"
    
    def toFailureIfNeeded(runResults: RunResults): Try[Unit] = {
      doToFailureIfNeeded(s"Couldn't copy '${localFile}' to '${remoteHostAndPath}'")(runResults)
    }

    runSync(s"${ebiScp} ${localFile} ${remoteHostAndPath}").flatMap(toFailureIfNeeded).map(toUnit)
  }
  
  private def copyFromEbi(remoteSrc: Path, localDest: Path): Try[Path] = {
    val remoteHostAndPath = s"${ebiUserAtHost}:${remoteSrc}"
    
    def toFailureIfNeeded(runResults: RunResults): Try[Unit] = {
      doToFailureIfNeeded(s"Couldn't copy '${remoteSrc}' to '${localDest}'")(runResults)
    }
    
    runSync(s"${ebiScp} ${remoteHostAndPath} ${localDest}").flatMap(toFailureIfNeeded).map(_ => localDest)
  }
  
  private def runAtEbi(remoteCommand: String, showOutput: Boolean = false): Try[Unit] = {
    def toFailureIfNeeded(runResults: RunResults): Try[Unit] = {
      doToFailureIfNeeded(s"Couldn't run remote command '${remoteCommand}' as/at ${ebiUserAtHost}")(runResults)
    }
    
    runSync(s"${ebiCli} ${remoteCommand}", showOutput).flatMap(toFailureIfNeeded)
  }
  
  private def copyInputFile(remoteDest: Path): Try[Unit] = copyToEbi(localResourceDir / "A.txt", remoteDest)
  
  private def runLoamStream(remoteWorkDir: Path): Try[Unit] = {
    val jarFile = remoteLsJarFilename
    val loamstreamConfFile = remoteLoamstreamConfFilename
    val loamFile = remoteLoamFilename
    val remoteLsfWorkDir = remoteWorkDir / "lsf"
    val remoteDotLoamstreamDir = remoteWorkDir / ".loamstream"

    val runLsCmd = {
      s""""cd ${remoteWorkDir} &&""" +
      //NB: Need to run bash in interactive mode (-i) so that bsub, bjobs, etc end up on the path :\
      "    bash -ic " + 
      s"""'java -Xss2m -jar ${jarFile} --conf ${loamstreamConfFile} --backend lsf --loams ${loamFile}'""""
    }
    
    for {
      _ <- runAtEbi(s"rm -f ${remoteWorkDir / "B.txt"} ${remoteWorkDir / "C.txt"}")
      _ <- runAtEbi(s"rm -rf ${remoteDotLoamstreamDir}")
      _ <- runAtEbi(s"rm -rf ${remoteLsfWorkDir}")
      _ <- runAtEbi(s"mkdir ${remoteLsfWorkDir}")
      _ <- runAtEbi(runLsCmd, showOutput = true)
    } yield ()
  }
  
  private def copyOutputFile(remoteOutputFile: Path, localDest: Path): Try[Path] = {
    copyFromEbi(remoteOutputFile, localDest)
  }
  
  private def runSync(
      commandLine: String, 
      streamOutput: Boolean = false)(implicit logCtx: LogContext): Try[RunResults] = {
    
    import java.nio.file.Paths.{ get => path }

    info(s"Running: '$commandLine'")
    
    Try {
      val processBuilder = BashScript.fromCommandLineString(commandLine).processBuilder(path("."))
    
      val bufferingProcessLogger = ProcessLoggers.buffering
      
      val processLogger = {
        if(streamOutput)  { new ProcessLoggers.PassThrough("LsAtEbiTest")(logCtx) } 
        else { bufferingProcessLogger }
      }
      
      val exitCode = processBuilder.!(processLogger)
    
      RunResults(commandLine, exitCode, bufferingProcessLogger.stdOut, bufferingProcessLogger.stdErr)
    }
  }
}
