package tools

import htsjdk.variant.variantcontext.Genotype
import loamstream.conf.{LProperties, SampleFiles}
import loamstream.utils.TestUtils
import utils.Loggable

/**
  * RugLoom - A prototype for a pipeline building toolkit
  * Created by oruebenacker on 3/8/16.
  */
object PcaProjectionApp extends App with Loggable {
  val pcaWeightsFile = TestUtils.assertSomeAndGet(PcaWeightsReader.weightsFilePath)
  val weights = PcaWeightsReader.read(pcaWeightsFile)
  val sampleFiles = SampleFiles(LProperties.Default)
  val miniVcf = TestUtils.assertSomeAndGet(sampleFiles.miniVcfOpt)
  val vcfParser = VcfParser(miniVcf)
  val samples = vcfParser.samples
  val pcaProjecter = PcaProjecter(weights)
  val genotypeToDouble: Genotype => Double = { genotype => VcfUtils.genotypeToAltCount(genotype).toDouble }
  val pcas = pcaProjecter.project(samples, vcfParser.genotypeMapIter, genotypeToDouble)
  debug(pcas.toString)
}
