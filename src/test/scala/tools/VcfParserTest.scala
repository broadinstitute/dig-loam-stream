package tools

import loamstream.conf.SampleFiles
import org.scalatest.FunSuite
import utils.TestUtils

/**
  * LoamStream
  * Created by oliverr on 3/8/2016.
  */
class VcfParserTest extends FunSuite {

  test("Reading a sample VCF file and count alt alleles") {
    val sampleVcfFile = TestUtils.assertSomeAndGet(SampleFiles.miniVcfOpt)
    val vcfParser = VcfParser(sampleVcfFile)
    val samples = vcfParser.samples
    assert(samples === Seq("Sample1", "Sample2", "Sample3"))
    val genotypes = vcfParser.genotypesIter.toSeq
    // scalastyle:off magic.number
    assert(VcfUtils.genotypeToAltCount(genotypes(2)(2)) === 2)
    assert(VcfUtils.genotypeToAltCount(genotypes(1).head) === 0)
    assert(VcfUtils.genotypeToAltCount(genotypes(4)(1)) === 1)
    val genotypesMaps = vcfParser.genotypeMapIter.toSeq
    assert(VcfUtils.genotypeToAltCount(genotypesMaps(2)("Sample3")) === 2)
    assert(VcfUtils.genotypeToAltCount(genotypesMaps(1)("Sample1")) === 0)
    assert(VcfUtils.genotypeToAltCount(genotypesMaps(4)("Sample2")) === 1)
    // scalastyle:on magic.number
  }

}
