package loamstream

import org.scalatest.FunSuite
import scala.util.Try
import loamstream.util.Processes
import loamstream.util.Loggable
import IntegrationTestHelpers.path
import java.nio.file.Path
import loamstream.util.ExitCodes
import loamstream.util.Sequence
import scala.util.Failure
import scala.util.Success
import loamstream.util.Files
import loamstream.util.RunResults
import loamstream.util.Tries
import loamstream.util.Paths
import loamstream.util.Paths.Implicits.PathHelpers

/**
 * @author clint
 * Jun 12, 2019
 */
final class LsAtEbiTest extends FunSuite {
  import LsAtEbiTest._
  
  test("Run a simple pipeline at EBI with some jobs in Singularity containers and some not") {
    val remoteWorkDir = remoteBaseDir / "simple-mixed-pipeline"
    
    val localWorkDir = IntegrationTestHelpers.getWorkDirUnderTarget()
    
    val outputFileAttempt = for {
      _ <- runAtEbi(s"rm -rf ${remoteWorkDir}")
      _ <- runAtEbi(s"mkdir -p ${remoteWorkDir}")
      _ <- copyLsJar(remoteWorkDir)
      _ <- copySimgFile(remoteWorkDir)
      _ <- copyLoamFile(remoteWorkDir)
      _ <- copyConfFile(remoteWorkDir)
      _ <- copyInputFile(remoteWorkDir)
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
  
  private def runSync(commandLine: String, showOutput: Boolean = false): Try[RunResults] = {
    info(s"Running: '$commandLine'")
    
    Processes.runSync(commandLine, showOutput)
  }
  
  private def copyToEbi(localFile: Path, remoteDest: Path): Try[Unit] = {
    val remoteHostAndPath = s"${ebiUserAtHost}:${remoteDest}"
    
    def toFailureIfNeeded(runResults: RunResults): Try[Unit] = {
      if(ExitCodes.isSuccess(runResults.exitCode)) { Success(()) }
      else { Tries.failure(s"Couldn't copy '${localFile}' to '${remoteHostAndPath}'") }
    }
    
    runSync(s"${ebiScp} ${localFile} ${remoteHostAndPath}").flatMap(toFailureIfNeeded).map(toUnit)
  }
  
  private def copyFromEbi(remoteSrc: Path, localDest: Path): Try[Path] = {
    val remoteHostAndPath = s"${ebiUserAtHost}:${remoteSrc}"
    
    def toFailureIfNeeded(runResults: RunResults): Try[Unit] = {
      if(ExitCodes.isSuccess(runResults.exitCode)) { Success(()) }
      else { Tries.failure(s"Couldn't copy '${remoteSrc}' to '${localDest}'") }
    }
    
    runSync(s"${ebiScp} ${remoteHostAndPath} ${localDest}").flatMap(toFailureIfNeeded).map(_ => localDest)
  }
  
  private def runAtEbi(remoteCommand: String, showOutput: Boolean = false): Try[Unit] = {
    def toFailureIfNeeded(runResults: RunResults): Try[Unit] = {
      if(ExitCodes.isSuccess(runResults.exitCode)) { Success(()) }
      else { Tries.failure(s"Couldn't run remote command '${remoteCommand}' as/at ${ebiUserAtHost}") }
    }
    
    runSync(s"${ebiCli} ${remoteCommand}", showOutput).flatMap(toFailureIfNeeded)
  }
  
  private def copyInputFile(remoteDest: Path): Try[Unit] = {
    copyToEbi(localResourceDir / "A.txt", remoteDest)
  }
  
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
}
