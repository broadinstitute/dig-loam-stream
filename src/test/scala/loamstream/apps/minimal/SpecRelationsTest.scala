package loamstream.apps.minimal

import java.nio.file.Paths

import loamstream.Sigs
import loamstream.model.{LId, Store, StoreOps, StoreSpec, Tool, ToolSpec}
import loamstream.tools.core.{CoreStore, CoreTool, LCoreDefaultStoreIds}
import org.scalatest.FunSuite

import scala.reflect.runtime.universe.typeOf

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

    assert(CoreStore.vcfFile.spec.tpe =:= coreGenotypeCallsStore.spec.tpe)
    assert(CoreStore.vcfFile.spec <:< coreGenotypeCallsStore.spec)

    //NB: Fragile
    val coreSampleIdsStore = CoreTool.ExtractSampleIdsFromVcfFile(vcfFile, sampleIdFile).outputs.head._2

    assert(CoreStore.sampleIdsFile.spec <:< coreSampleIdsStore.spec)

  }
}
