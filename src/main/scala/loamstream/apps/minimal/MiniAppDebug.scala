package loamstream.apps.minimal

import tools.core.CoreStore
import utils.Loggable

/**
  * RugLoom - A prototype for a pipeline building toolkit
  * Created by oruebenacker on 2/22/16.
  */
object MiniAppDebug extends Loggable {

  def thisShouldBeFalse(test: Boolean): Unit = debug("This should be false: " + test)

  def theseShouldAllBeFalse(): Unit = {
    thisShouldBeFalse(CoreStore.vcfFile.pile <:< MiniMockStore.genotypesCassandraTable.pile)
    thisShouldBeFalse(
      MiniTool.checkPreExistingVcfFile.recipe <:< MiniTool.checkPreExistingGenotypeCassandraTable.recipe)
    thisShouldBeFalse(MiniMockStore.genotypesCassandraTable.pile <:< CoreStore.vcfFile.pile)
    thisShouldBeFalse(
      MiniTool.checkPreExistingGenotypeCassandraTable.recipe <:< MiniTool.checkPreExistingVcfFile.recipe)
    thisShouldBeFalse(CoreStore.sampleIdsFile.pile <:< MiniMockStore.sampleIdsCassandraTable.pile)
    thisShouldBeFalse(
      MiniTool.extractSampleIdsFromVcfFile.recipe <:< MiniTool.extractSampleIdsFromCassandraTable.recipe)
    thisShouldBeFalse(MiniMockStore.sampleIdsCassandraTable.pile <:< CoreStore.sampleIdsFile.pile)
    thisShouldBeFalse(
      MiniTool.extractSampleIdsFromCassandraTable.recipe <:< MiniTool.extractSampleIdsFromVcfFile.recipe)
  }

  def thisShouldBeTrue(test: Boolean): Unit = debug("This should be true: " + test)

  def theseShouldAllBeTrue(): Unit = {
    thisShouldBeTrue(CoreStore.vcfFile.pile <:< MiniPipeline.genotypeCallsPile.spec)
    thisShouldBeTrue(MiniMockStore.genotypesCassandraTable.pile <:< MiniPipeline.genotypeCallsPile.spec)
    thisShouldBeTrue(CoreStore.sampleIdsFile.pile <:< MiniPipeline.sampleIdsPile.spec)
    thisShouldBeTrue(MiniMockStore.sampleIdsCassandraTable.pile <:< MiniPipeline.sampleIdsPile.spec)
    thisShouldBeTrue(MiniTool.checkPreExistingVcfFile.recipe <<< MiniPipeline.genotypeCallsRecipe.spec)
    thisShouldBeTrue(
      MiniTool.checkPreExistingGenotypeCassandraTable.recipe <<< MiniPipeline.genotypeCallsRecipe.spec)
    thisShouldBeTrue(MiniTool.extractSampleIdsFromVcfFile.recipe <<< MiniPipeline.sampleIdsRecipe.spec)
    thisShouldBeTrue(MiniTool.extractSampleIdsFromCassandraTable.recipe <<< MiniPipeline.sampleIdsRecipe.spec)
  }

}
