package tools

import java.io.File

import org.scalatest.FunSuite

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future, blocking}
import scala.sys.process.{ProcessLogger, Process}

/**
  * LoamStream
  * Created by oliverr on 3/16/2016.
  */
class KlustaKwikRunTest extends FunSuite {
  test("Running KlustaKwik, assert exit value is zero. That's all for now.") {
    val fileBaseVal = "data"
    val workDirName = "klusta"
    val workDir = new File(workDirName)
    if (!workDir.exists) {
      assume(workDir.mkdir)
    }
    assume(workDir.isDirectory)
    val nSamples = 100
    val nPcas = 7
    val pcaMin = 0.0
    val pcaMax = 1.0
    val data = KlustaKwikMockDataGenerator.generate(nSamples, nPcas, pcaMin, pcaMax)
    val iShankVal = 1
    KlustaKwikInputWriter.writeFeatures(workDir, fileBaseVal, iShankVal, data)
    val command = {
      import KlustaKwikLineCommand._
      KlustaKwikLineCommand + fileBase(fileBaseVal) + elecNo(iShankVal) + useDistributional(0)
    }
    val exitValueFuture = Future {
      blocking {
        val noOpProcessLogger = ProcessLogger(line => ())
        Process(command.tokens, workDir).run(noOpProcessLogger).exitValue
      }
    }
    val timeOut = 10.seconds
    val exitValue = Await.result(exitValueFuture, timeOut)
    val exitValueOnSuccess = 0
    assert(exitValue === exitValueOnSuccess)
  }
}
