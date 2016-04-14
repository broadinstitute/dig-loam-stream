package loamstream.tools

import htsjdk.variant.variantcontext.Genotype
import loamstream.TestData.sampleFiles
import loamstream.TestHelpers
import org.scalatest.FunSuite
import loamstream.TestData

/**
  * RugLoom - A prototype for a pipeline building toolkit
  * Created by oruebenacker on 3/8/16.
  */
final class PcaProjectionTest extends FunSuite {
  test("PCA projection seems to work.") {
    val props = TestData.props
    
    val pcaWeightsFile = PcaWeightsReader.weightsFilePath(props).get
    val weights = PcaWeightsReader.read(pcaWeightsFile)
    val miniVcf = sampleFiles.miniForPcaVcfOpt.get
    val vcfParser = VcfParser(miniVcf)
    val samples = vcfParser.samples
    val pcaProjecter = PcaProjecter(weights)
    val genotypeToDouble: Genotype => Double = { genotype => VcfUtils.genotypeToAltCount(genotype).toDouble }
    val pcaProjections = pcaProjecter.project(samples, vcfParser.genotypeMapIter, genotypeToDouble)
    val nSamples = 3
    val nPca = 10
    assert(pcaProjections.size == nSamples)
    for (sampleProjection <- pcaProjections) {
      assert(sampleProjection.size == nPca)
    }
    assert(TestHelpers.areWithinExpectedError(pcaProjections.head.head, 0.049000000000000044))
    assert(TestHelpers.areWithinExpectedError(pcaProjections(1)(1), -2.705))
    assert(TestHelpers.areWithinExpectedError(pcaProjections(2)(2), 0.5379999999999999))
  }
}
