package tools

import loamstream.conf.SampleFiles
import loamstream.utils.TestUtils
import org.scalatest.FunSuite

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
    val seqRows = vcfParser.genotypesIter.toSeq
    // scalastyle:off magic.number
    assert(VcfUtils.genotypeToAltCount(seqRows(2).genotypes(2)) === 2)
    assert(VcfUtils.genotypeToAltCount(seqRows(1).genotypes.head) === 0)
    assert(VcfUtils.genotypeToAltCount(seqRows(4).genotypes(1)) === 1)
    val mapRows = vcfParser.genotypeMapIter.toSeq
    assert(VcfUtils.genotypeToAltCount(mapRows(2).genotypesMap("Sample3")) === 2)
    assert(VcfUtils.genotypeToAltCount(mapRows(1).genotypesMap("Sample1")) === 0)
    assert(VcfUtils.genotypeToAltCount(mapRows(4).genotypesMap("Sample2")) === 1)
    // scalastyle:on magic.number
  }

}
