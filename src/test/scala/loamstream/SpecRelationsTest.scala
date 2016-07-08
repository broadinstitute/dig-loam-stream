package loamstream

import java.nio.file.Paths
import loamstream.tools.core.{CoreStore, CoreTool}
import org.scalatest.FunSuite

/**
  * RugLoom - A prototype for a pipeline building toolkit
  * Created by oruebenacker on 2/22/16.
  */
final class SpecRelationsTest extends FunSuite {

  private val vcfFile = Paths.get("foo.vcf")
  private val sampleIdFile = Paths.get("foo.vcf")

  test("Various relations between store and tool specs are true as expected") {

    //NB: Fragile
    val coreGenotypeCallsStore = CoreTool.CheckPreExistingVcfFile(vcfFile).outputs.head._2

    assert(CoreStore.vcfFile.sig.tpe =:= coreGenotypeCallsStore.sig.tpe)
    assert(CoreStore.vcfFile.sig <:< coreGenotypeCallsStore.sig)

    //NB: Fragile
    val coreSampleIdsStore = CoreTool.ExtractSampleIdsFromVcfFile(vcfFile, sampleIdFile).outputs.head._2

    assert(CoreStore.sampleIdsFile.sig <:< coreSampleIdsStore.sig)

  }
}
