package loamstream.apps.minimal

import tools.core.{CoreStore, CoreTool}
import utils.Loggable

/**
  * RugLoom - A prototype for a pipeline building toolkit
  * Created by oruebenacker on 2/22/16.
  */
object MiniAppDebug extends Loggable {

  def thisShouldBeFalse(test: Boolean): Unit = debug("This should be false: " + test)

  def theseShouldAllBeFalse(genotypeId: String): Unit = {
    thisShouldBeFalse(CoreStore.vcfFile.pile <:< MiniMockStore.genotypesCassandraTable.pile)
    thisShouldBeFalse(
      CoreTool.checkPreExistingVcfFile(genotypeId).recipe <:<
        MiniMockTool.checkPreExistingGenotypeCassandraTable(genotypeId).recipe)
    thisShouldBeFalse(MiniMockStore.genotypesCassandraTable.pile <:< CoreStore.vcfFile.pile)
    thisShouldBeFalse(
      MiniMockTool.checkPreExistingGenotypeCassandraTable(genotypeId).recipe <:<
        CoreTool.checkPreExistingVcfFile(genotypeId).recipe)
    thisShouldBeFalse(CoreStore.sampleIdsFile.pile <:< MiniMockStore.sampleIdsCassandraTable.pile)
    thisShouldBeFalse(
      CoreTool.extractSampleIdsFromVcfFile.recipe <:< MiniMockTool.extractSampleIdsFromCassandraTable.recipe)
    thisShouldBeFalse(MiniMockStore.sampleIdsCassandraTable.pile <:< CoreStore.sampleIdsFile.pile)
    thisShouldBeFalse(
      MiniMockTool.extractSampleIdsFromCassandraTable.recipe <:< CoreTool.extractSampleIdsFromVcfFile.recipe)
  }

  def thisShouldBeTrue(test: Boolean): Unit = debug("This should be true: " + test)

  def theseShouldAllBeTrue(genotypeId: String): Unit = {
    val pipeline = MiniPipeline(genotypeId)
    thisShouldBeTrue(CoreStore.vcfFile.pile <:< pipeline.genotypeCallsPile.spec)
    thisShouldBeTrue(MiniMockStore.genotypesCassandraTable.pile <:< pipeline.genotypeCallsPile.spec)
    thisShouldBeTrue(CoreStore.sampleIdsFile.pile <:< pipeline.sampleIdsPile.spec)
    thisShouldBeTrue(MiniMockStore.sampleIdsCassandraTable.pile <:< pipeline.sampleIdsPile.spec)
    thisShouldBeTrue(CoreTool.checkPreExistingVcfFile(genotypeId).recipe <<< pipeline.genotypeCallsRecipe.spec)
    thisShouldBeTrue(
      MiniMockTool.checkPreExistingGenotypeCassandraTable(genotypeId).recipe <<< pipeline.genotypeCallsRecipe.spec)
    thisShouldBeTrue(CoreTool.extractSampleIdsFromVcfFile.recipe <<< pipeline.sampleIdsRecipe.spec)
    thisShouldBeTrue(MiniMockTool.extractSampleIdsFromCassandraTable.recipe <<< pipeline.sampleIdsRecipe.spec)
  }

}
