package loamstream.apps.minimal

import loamstream.tools.core.{CoreStore, CoreTool, LCoreDefaultPileIds}
import loamstream.util.Loggable
import org.scalatest.FunSuite

/**
  * RugLoom - A prototype for a pipeline building toolkit
  * Created by oruebenacker on 2/22/16.
  */
class  SpecRelationsTest extends FunSuite {

  test("Various relations between pile and recipe specs are false as expected") {
    val genotypeId = LCoreDefaultPileIds.genotypes
    assertResult(false)(CoreStore.vcfFile.pile <:< MiniMockStore.genotypesCassandraTable.pile)
    assertResult(false)(
      CoreTool.checkPreExistingVcfFile(genotypeId).recipe <:<
        MiniMockTool.checkPreExistingGenotypeCassandraTable(genotypeId).recipe)
    assertResult(false)(MiniMockStore.genotypesCassandraTable.pile <:< CoreStore.vcfFile.pile)
    assertResult(false)(
      MiniMockTool.checkPreExistingGenotypeCassandraTable(genotypeId).recipe <:<
        CoreTool.checkPreExistingVcfFile(genotypeId).recipe)
    assertResult(false)(CoreStore.sampleIdsFile.pile <:< MiniMockStore.sampleIdsCassandraTable.pile)
    assertResult(false)(
      CoreTool.extractSampleIdsFromVcfFile.recipe <:< MiniMockTool.extractSampleIdsFromCassandraTable.recipe)
    assertResult(false)(MiniMockStore.sampleIdsCassandraTable.pile <:< CoreStore.sampleIdsFile.pile)
    assertResult(false)(
      MiniMockTool.extractSampleIdsFromCassandraTable.recipe <:< CoreTool.extractSampleIdsFromVcfFile.recipe)
  }

  test("Various relations between pile and recipe specs are true as expected") {
    val genotypeId = LCoreDefaultPileIds.genotypes
    val pipeline = MiniPipeline(genotypeId)
    assert(CoreStore.vcfFile.pile.sig =:= pipeline.genotypeCallsPile.spec.sig)
    assert(CoreStore.vcfFile.pile <:< pipeline.genotypeCallsPile.spec)
    assert(MiniMockStore.genotypesCassandraTable.pile <:< pipeline.genotypeCallsPile.spec)
    assert(CoreStore.sampleIdsFile.pile <:< pipeline.sampleIdsPile.spec)
    assert(MiniMockStore.sampleIdsCassandraTable.pile <:< pipeline.sampleIdsPile.spec)
    assert(CoreTool.checkPreExistingVcfFile(genotypeId).recipe <<< pipeline.genotypeCallsRecipe.spec)
    assert(
      MiniMockTool.checkPreExistingGenotypeCassandraTable(genotypeId).recipe <<< pipeline.genotypeCallsRecipe.spec)
    assert(CoreTool.extractSampleIdsFromVcfFile.recipe <<< pipeline.sampleIdsRecipe.spec)
    assert(MiniMockTool.extractSampleIdsFromCassandraTable.recipe <<< pipeline.sampleIdsRecipe.spec)
  }

}
