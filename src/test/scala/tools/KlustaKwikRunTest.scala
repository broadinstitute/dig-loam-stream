package tools

import java.nio.file.Files

import org.scalatest.FunSuite

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future, blocking}
import scala.sys.process.{Process, ProcessLogger}

/**
  * LoamStream
  * Created by oliverr on 3/16/2016.
  */
class KlustaKwikRunTest extends FunSuite {
  test("Running KlustaKwik with mock data, checking output sanity.") {
    val fileBaseVal = "data"
    val workDir = Files.createTempDirectory("klusta")
    val nSamples = 1000
    val nPcas = 10
    val nClusters = 7
    val data = KlustaKwikMockDataGenerator.generate(nSamples, nPcas, nClusters)
    val iShankVal = 1
    KlustaKwikInputWriter.writeFeatures(workDir, fileBaseVal, iShankVal, data)
    val command = {
      import KlustaKwikLineCommand._
      KlustaKwikLineCommand + fileBase(fileBaseVal) + elecNo(iShankVal) + useDistributional(0)
    }
    val exitValueFuture = Future {
      blocking {
        val noOpProcessLogger = ProcessLogger(line => ())
        Process(command.tokens, workDir.toFile).run(noOpProcessLogger).exitValue
      }
    }
    val timeOut = 10.seconds
    val exitValue = Await.result(exitValueFuture, timeOut)
    val exitValueOnSuccess = 0
    assert(exitValue === exitValueOnSuccess)
    val klustaOutput = KlustaKwikOutputReader.read(workDir, fileBaseVal, iShankVal).get
    val nClustersDetected = klustaOutput.nClusters
    val nClustersDetectedMin = 4
    val nClustersDetectedMax = 8
    assert(nClustersDetectedMin <= nClustersDetected)
    assert(nClustersDetected <= nClustersDetectedMax)
    val nSamplesInOutput = klustaOutput.clustering.size
    assert(nSamplesInOutput === nSamples)
    val largestICluster = klustaOutput.clustering.max
    assert(largestICluster === nClustersDetected)
  }
}
