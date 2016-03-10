package tools

import loamstream.conf.SampleFiles
import loamstream.utils.TestUtils
import utils.Loggable

/**
  * RugLoom - A prototype for a pipeline building toolkit
  * Created by oruebenacker on 3/8/16.
  */
object PcaProjectionApp extends App with Loggable {
  val pcaWeightsFile = TestUtils.assertSomeAndGet(PcaWeightsReader.weightsFilePath)
  val weights = PcaWeightsReader.read(pcaWeightsFile)
  val miniVcf = TestUtils.assertSomeAndGet(SampleFiles.miniVcfOpt)
  val vcfParser = VcfParser(miniVcf)
  for (genotypeMap <- vcfParser.genotypeMapIter) {
    // TODO
  }
}
