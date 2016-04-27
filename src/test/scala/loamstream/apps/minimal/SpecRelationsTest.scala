package loamstream.apps.minimal

import loamstream.tools.core.{CoreStore, CoreTool, LCoreDefaultPileIds}
import loamstream.util.Loggable
import org.scalatest.FunSuite

/**
  * RugLoom - A prototype for a pipeline building toolkit
  * Created by oruebenacker on 2/22/16.
  */
final class SpecRelationsTest extends FunSuite {

  test("Various relations between pile and recipe specs are false as expected") {
    val genotypeId = LCoreDefaultPileIds.genotypes
    assertResult(false)(CoreStore.vcfFile.spec <:< MiniMockStore.genotypesCassandraTable.spec)
    assertResult(false)(
      CoreTool.checkPreExistingVcfFile(genotypeId).spec <:<
        MiniMockTool.checkPreExistingGenotypeCassandraTable(genotypeId).spec)
    assertResult(false)(MiniMockStore.genotypesCassandraTable.spec <:< CoreStore.vcfFile.spec)
    assertResult(false)(
      MiniMockTool.checkPreExistingGenotypeCassandraTable(genotypeId).spec <:<
        CoreTool.checkPreExistingVcfFile(genotypeId).spec)
    assertResult(false)(CoreStore.sampleIdsFile.spec <:< MiniMockStore.sampleIdsCassandraTable.spec)
    assertResult(false)(
      CoreTool.extractSampleIdsFromVcfFile.spec <:< MiniMockTool.extractSampleIdsFromCassandraTable.spec)
    assertResult(false)(MiniMockStore.sampleIdsCassandraTable.spec <:< CoreStore.sampleIdsFile.spec)
    assertResult(false)(
      MiniMockTool.extractSampleIdsFromCassandraTable.spec <:< CoreTool.extractSampleIdsFromVcfFile.spec)
  }

  test("Various relations between pile and recipe specs are true as expected") {
    val genotypeId = LCoreDefaultPileIds.genotypes
    val pipeline = MiniPipeline(genotypeId)
    assert(CoreStore.vcfFile.spec.sig =:= pipeline.genotypeCallsPile.spec.sig)
    assert(CoreStore.vcfFile.spec <:< pipeline.genotypeCallsPile.spec)
    assert(MiniMockStore.genotypesCassandraTable.spec <:< pipeline.genotypeCallsPile.spec)
    assert(CoreStore.sampleIdsFile.spec <:< pipeline.sampleIdsPile.spec)
    assert(MiniMockStore.sampleIdsCassandraTable.spec <:< pipeline.sampleIdsPile.spec)
    assert(CoreTool.checkPreExistingVcfFile(genotypeId).spec <<< pipeline.genotypeCallsRecipe.spec)
    assert(
      MiniMockTool.checkPreExistingGenotypeCassandraTable(genotypeId).spec <<< pipeline.genotypeCallsRecipe.spec)
    assert(CoreTool.extractSampleIdsFromVcfFile.spec <<< pipeline.sampleIdsRecipe.spec)
    assert(MiniMockTool.extractSampleIdsFromCassandraTable.spec <<< pipeline.sampleIdsRecipe.spec)
  }
}
