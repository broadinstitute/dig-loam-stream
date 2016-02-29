package loamstream.apps.minimal

/**
 * RugLoom - A prototype for a pipeline building toolkit
 * Created by oruebenacker on 2/22/16.
 */
object MiniAppDebug {

  def theseShouldAllBeFalse() : Unit = {
    println(MiniStore.vcfFile.pile <:< MiniStore.genotypesCassandraTable.pile)
    println(
      MiniTool.checkPreExistingVcfFile.recipe <:< MiniTool.checkPreExistingGenotypeCassandraTable.recipe)
    println(MiniStore.genotypesCassandraTable.pile <:< MiniStore.vcfFile.pile)
    println(
      MiniTool.checkPreExistingGenotypeCassandraTable.recipe <:< MiniTool.checkPreExistingVcfFile.recipe)
    println(MiniStore.sampleIdsFile.pile <:< MiniStore.sampleIdsCassandraTable.pile)
    println(
      MiniTool.extractSampleIdsFromVcfFile.recipe <:< MiniTool.extractSampleIdsFromCassandraTable.recipe)
    println(MiniStore.sampleIdsCassandraTable.pile <:< MiniStore.sampleIdsFile.pile)
    println(
      MiniTool.extractSampleIdsFromCassandraTable.recipe <:< MiniTool.extractSampleIdsFromVcfFile.recipe)
  }

  def theseShouldAllBeTrue() : Unit = {
    println(MiniStore.vcfFile.pile <:< MiniPipeline.genotypeCallsPile.spec)
    println(MiniStore.genotypesCassandraTable.pile <:< MiniPipeline.genotypeCallsPile.spec)
    println(MiniStore.sampleIdsFile.pile <:< MiniPipeline.sampleIdsPile.spec)
    println(MiniStore.sampleIdsCassandraTable.pile <:< MiniPipeline.sampleIdsPile.spec)
    println(MiniTool.checkPreExistingVcfFile.recipe <<< MiniPipeline.genotypeCallsRecipe.spec)
    println(MiniTool.checkPreExistingGenotypeCassandraTable.recipe <<< MiniPipeline.genotypeCallsRecipe.spec)
    println(MiniTool.extractSampleIdsFromVcfFile.recipe <<< MiniPipeline.sampleIdsRecipe.spec)
    println(MiniTool.extractSampleIdsFromCassandraTable.recipe <<< MiniPipeline.sampleIdsRecipe.spec)
  }

}
