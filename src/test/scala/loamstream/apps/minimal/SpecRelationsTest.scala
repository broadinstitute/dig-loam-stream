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
      CoreTool.checkPreExistingVcfFile(genotypeId).recipe <:<
        MiniMockTool.checkPreExistingGenotypeCassandraTable(genotypeId).recipe)
    assertResult(false)(MiniMockStore.genotypesCassandraTable.spec <:< CoreStore.vcfFile.spec)
    assertResult(false)(
      MiniMockTool.checkPreExistingGenotypeCassandraTable(genotypeId).recipe <:<
        CoreTool.checkPreExistingVcfFile(genotypeId).recipe)
    assertResult(false)(CoreStore.sampleIdsFile.spec <:< MiniMockStore.sampleIdsCassandraTable.spec)
    assertResult(false)(
      CoreTool.extractSampleIdsFromVcfFile.recipe <:< MiniMockTool.extractSampleIdsFromCassandraTable.recipe)
    assertResult(false)(MiniMockStore.sampleIdsCassandraTable.spec <:< CoreStore.sampleIdsFile.spec)
    assertResult(false)(
      MiniMockTool.extractSampleIdsFromCassandraTable.recipe <:< CoreTool.extractSampleIdsFromVcfFile.recipe)
  }

  test("Various relations between pile and recipe specs are true as expected") {
    val genotypeId = LCoreDefaultPileIds.genotypes
    val pipeline = MiniPipeline(genotypeId)
    assert(CoreStore.vcfFile.spec.sig =:= pipeline.genotypeCallsPile.spec.sig)
    assert(CoreStore.vcfFile.spec <:< pipeline.genotypeCallsPile.spec)
    assert(MiniMockStore.genotypesCassandraTable.spec <:< pipeline.genotypeCallsPile.spec)
    assert(CoreStore.sampleIdsFile.spec <:< pipeline.sampleIdsPile.spec)
    assert(MiniMockStore.sampleIdsCassandraTable.spec <:< pipeline.sampleIdsPile.spec)
    assert(CoreTool.checkPreExistingVcfFile(genotypeId).recipe <<< pipeline.genotypeCallsRecipe.spec)
    assert(
      MiniMockTool.checkPreExistingGenotypeCassandraTable(genotypeId).recipe <<< pipeline.genotypeCallsRecipe.spec)
    assert(CoreTool.extractSampleIdsFromVcfFile.recipe <<< pipeline.sampleIdsRecipe.spec)
    assert(MiniMockTool.extractSampleIdsFromCassandraTable.recipe <<< pipeline.sampleIdsRecipe.spec)
  }
}
