package loamstream.tools.klusta

import java.io.IOException

import scala.concurrent.{ Await, Future, blocking }
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import scala.sys.process.{ Process, ProcessLogger }

import org.scalatest.FunSuite

import KlustaKwikLineCommand.useDistributional

/**
  * LoamStream
  * Created by oliverr on 3/16/2016.
  */
final class KlustaKwikRunTest extends FunSuite {
  test("Running KlustaKwik with mock data, checking output sanity.") {
    val fileBaseVal = "data"
    val konfig = KlustaKwikKonfig.withTempWorkDir(fileBaseVal)
    val nSamples = 1000
    val nPcas = 10
    val nClusters = 7
    val data = KlustaKwikMockDataGenerator.generate(nSamples, nPcas, nClusters)
    KlustaKwikInputWriter.writeFeatures(konfig, data)
    val command = {
      import KlustaKwikLineCommand.{klustaKwik, useDistributional}
      klustaKwik(konfig) + useDistributional(0)
    }
    val exitValueFuture = Future {
      blocking {
        val noOpProcessLogger = ProcessLogger(line => ())
        try {
          Process(command.tokens, konfig.workDir.toFile).run(noOpProcessLogger).exitValue
        } catch {
          case ex: IOException => cancel(ex)
          case ex: SecurityException => cancel(ex)
        }
      }
    }
    val timeOut = 15.seconds
    val exitValue = Await.result(exitValueFuture, timeOut)
    val exitValueOnSuccess = 0
    assert(exitValue === exitValueOnSuccess)
    val klustaOutput = KlustaKwikOutputReader.read(konfig).get
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
