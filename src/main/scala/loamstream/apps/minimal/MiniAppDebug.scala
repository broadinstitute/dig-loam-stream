package loamstream.apps.minimal

/**
 * RugLoom - A prototype for a pipeline building toolkit
 * Created by oruebenacker on 2/22/16.
 */
object MiniAppDebug {

  def theseShouldAllBeFalse() : Unit = {
    println(MiniStore.vcfFile.pile.spec <:< MiniStore.genotypesCassandraTable.pile.spec)
    println(
      MiniTool.checkPreExistingVcfFile.recipe <:< MiniTool.checkPreExistingGenotypeCassandraTable.recipe)
    println(MiniStore.genotypesCassandraTable.pile.spec <:< MiniStore.vcfFile.pile.spec)
    println(
      MiniTool.checkPreExistingGenotypeCassandraTable.recipe <:< MiniTool.checkPreExistingVcfFile.recipe)
    println(MiniStore.sampleIdsFile.pile.spec <:< MiniStore.sampleIdsCassandraTable.pile.spec)
    println(
      MiniTool.extractSampleIdsFromVcfFile.recipe <:< MiniTool.extractSampleIdsFromCassandraTable.recipe)
    println(MiniStore.sampleIdsCassandraTable.pile.spec <:< MiniStore.sampleIdsFile.pile.spec)
    println(
      MiniTool.extractSampleIdsFromCassandraTable.recipe <:< MiniTool.extractSampleIdsFromVcfFile.recipe)
  }

  def theseShouldAllBeTrue() : Unit = {
    println(MiniStore.vcfFile.pile.spec <:< MiniPipeline.genotypeCallsPile.spec)
    println(MiniStore.genotypesCassandraTable.pile.spec <:< MiniPipeline.genotypeCallsPile.spec)
    println(MiniStore.sampleIdsFile.pile.spec <:< MiniPipeline.sampleIdsPile.spec)
    println(MiniStore.sampleIdsCassandraTable.pile.spec <:< MiniPipeline.sampleIdsPile.spec)
    println(MiniTool.checkPreExistingVcfFile.recipe <<< MiniPipeline.genotypeCallsRecipe)
    println(MiniTool.checkPreExistingGenotypeCassandraTable.recipe <<< MiniPipeline.genotypeCallsRecipe)
    println(MiniTool.extractSampleIdsFromVcfFile.recipe <<< MiniPipeline.sampleIdsRecipe)
    println(MiniTool.extractSampleIdsFromCassandraTable.recipe <<< MiniPipeline.sampleIdsRecipe)
  }

}
