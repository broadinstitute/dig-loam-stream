package loamstream.apps.qc

import loamstream.conf.SampleFiles
import loamstream.utils.TestUtils
import tools.VcfParser
import utils.Loggable

/**
  * LoamStream
  * Created by oliverr on 3/2/2016.
  */
object ReadVcfApp extends App with Loggable {
  val sampleVcfFile = TestUtils.assertSomeAndGet(SampleFiles.miniVcfOpt)
  val vcfParser = VcfParser(sampleVcfFile)
  val samples = vcfParser.readSamples
  debug(samples.toString)

  val firstRow = vcfParser.rowIterator.next()
  debug(firstRow.toString)
}
