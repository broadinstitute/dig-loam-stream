package tools

import java.io.File

import org.scalatest.FunSuite

import scala.sys.process.{Process, ProcessLogger}

/**
  * LoamStream
  * Created by oliverr on 3/16/2016.
  */
class KlustaKwikRunTest extends FunSuite {
  test("Running KlustaKwik. That's all for now.") {
    val fileBaseVal = "data"
    val workDirName = "klusta"
    val workDir = new File(workDirName)
    if (!workDir.exists) {
      assume(workDir.mkdir)
    }
    assume(workDir.isDirectory)
    val nSamples = 3
    val nPcas = 5
    val pcaMin = 0.0
    val pcaMax = 1.0
    val data = KlustaKwikMockDataGenerator.generate(nSamples, nPcas, pcaMin, pcaMax)
    val iShankVal = 1
    KlustaKwikInputWriter.writeFeatures(workDir, fileBaseVal, iShankVal, data)
    val command = {
      import KlustaKwikLineCommand._
      KlustaKwikLineCommand + fileBase(fileBaseVal) + elecNo(iShankVal)
    }
    val process = Process(command.tokens, workDir)
    val printLines: String => Unit = line => println(line) // scalastyle:ignore
    process.lineStream_!(ProcessLogger(printLines)).foreach(printLines)
  }
}
