package loamstream.apps.qc

import loamstream.conf.SampleFiles
import loamstream.utils.TestUtils
import tools.{VcfParser, VcfUtils}
import utils.Loggable

/**
  * LoamStream
  * Created by oliverr on 3/2/2016.
  */
object ReadVcfApp extends App with Loggable {
  val sampleVcfFile = TestUtils.assertSomeAndGet(SampleFiles.miniVcfOpt)
  val vcfParser = VcfParser(sampleVcfFile)
  val samples = vcfParser.samples
  debug(samples.toString)

  for (row <- vcfParser.rowIter) {
    debug(row.toString)
  }
  for (row <- vcfParser.genotypesIter) {
    debug(row.map(VcfUtils.genotypeToAltCount).toString)
  }
  for (row <- vcfParser.genotypeMapIter) {
    debug(row.mapValues(VcfUtils.genotypeToAltCount).toString)
  }
}
