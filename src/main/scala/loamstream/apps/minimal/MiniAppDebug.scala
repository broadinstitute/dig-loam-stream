package loamstream.apps.minimal

import utils.Loggable

/**
  * RugLoom - A prototype for a pipeline building toolkit
  * Created by oruebenacker on 2/22/16.
  */
object MiniAppDebug extends Loggable {

  def thisShouldBeFalse(test: Boolean): Unit = debug("This should be false: " + test)

  def theseShouldAllBeFalse(): Unit = {
    thisShouldBeFalse(MiniStore.vcfFile.pile <:< MiniStore.genotypesCassandraTable.pile)
    thisShouldBeFalse(
      MiniTool.checkPreExistingVcfFile.recipe <:< MiniTool.checkPreExistingGenotypeCassandraTable.recipe)
    thisShouldBeFalse(MiniStore.genotypesCassandraTable.pile <:< MiniStore.vcfFile.pile)
    thisShouldBeFalse(
      MiniTool.checkPreExistingGenotypeCassandraTable.recipe <:< MiniTool.checkPreExistingVcfFile.recipe)
    thisShouldBeFalse(MiniStore.sampleIdsFile.pile <:< MiniStore.sampleIdsCassandraTable.pile)
    thisShouldBeFalse(
      MiniTool.extractSampleIdsFromVcfFile.recipe <:< MiniTool.extractSampleIdsFromCassandraTable.recipe)
    thisShouldBeFalse(MiniStore.sampleIdsCassandraTable.pile <:< MiniStore.sampleIdsFile.pile)
    thisShouldBeFalse(
      MiniTool.extractSampleIdsFromCassandraTable.recipe <:< MiniTool.extractSampleIdsFromVcfFile.recipe)
  }

  def thisShouldBeTrue(test: Boolean): Unit = debug("This should be true: " + test)

  def theseShouldAllBeTrue(): Unit = {
    thisShouldBeTrue(MiniStore.vcfFile.pile <:< MiniPipeline.genotypeCallsPile.spec)
    thisShouldBeTrue(MiniStore.genotypesCassandraTable.pile <:< MiniPipeline.genotypeCallsPile.spec)
    thisShouldBeTrue(MiniStore.sampleIdsFile.pile <:< MiniPipeline.sampleIdsPile.spec)
    thisShouldBeTrue(MiniStore.sampleIdsCassandraTable.pile <:< MiniPipeline.sampleIdsPile.spec)
    thisShouldBeTrue(MiniTool.checkPreExistingVcfFile.recipe <<< MiniPipeline.genotypeCallsRecipe.spec)
    thisShouldBeTrue(
      MiniTool.checkPreExistingGenotypeCassandraTable.recipe <<< MiniPipeline.genotypeCallsRecipe.spec)
    thisShouldBeTrue(MiniTool.extractSampleIdsFromVcfFile.recipe <<< MiniPipeline.sampleIdsRecipe.spec)
    thisShouldBeTrue(MiniTool.extractSampleIdsFromCassandraTable.recipe <<< MiniPipeline.sampleIdsRecipe.spec)
  }

}
