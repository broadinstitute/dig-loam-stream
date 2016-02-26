package loamstream.apps.minimal

/**
 * RugLoom - A prototype for a pipeline building toolkit
 * Created by oruebenacker on 2/22/16.
 */
object MiniAppDebug {

  def theseShouldAllBeFalse() : Unit = {
    println(MiniStore.vcfFile.pileSpec <:< MiniStore.genotypesCassandraTable.pileSpec)
    println(
      MiniTool.checkPreExistingVcfFile.recipeSpec <:< MiniTool.checkPreExistingGenotypeCassandraTable.recipeSpec)
    println(MiniStore.genotypesCassandraTable.pileSpec <:< MiniStore.vcfFile.pileSpec)
    println(
      MiniTool.checkPreExistingGenotypeCassandraTable.recipeSpec <:< MiniTool.checkPreExistingVcfFile.recipeSpec)
    println(MiniStore.sampleIdsFile.pileSpec <:< MiniStore.sampleIdsCassandraTable.pileSpec)
    println(
      MiniTool.extractSampleIdsFromVcfFile.recipeSpec <:< MiniTool.extractSampleIdsFromCassandraTable.recipeSpec)
    println(MiniStore.sampleIdsCassandraTable.pileSpec <:< MiniStore.sampleIdsFile.pileSpec)
    println(
      MiniTool.extractSampleIdsFromCassandraTable.recipeSpec <:< MiniTool.extractSampleIdsFromVcfFile.recipeSpec)
  }

  def theseShouldAllBeTrue() : Unit = {
    println(MiniStore.vcfFile.pileSpec <:< MiniPipeline.genotypeCallsPile.spec)
    println(MiniStore.genotypesCassandraTable.pileSpec <:< MiniPipeline.genotypeCallsPile.spec)
    println(MiniStore.sampleIdsFile.pileSpec <:< MiniPipeline.sampleIdsPile.spec)
    println(MiniStore.sampleIdsCassandraTable.pileSpec <:< MiniPipeline.sampleIdsPile.spec)
    println(MiniTool.checkPreExistingVcfFile.recipe.spec <<< MiniPipeline.genotypeCallsRecipe.spec)
    println(MiniTool.checkPreExistingGenotypeCassandraTable.recipe.spec <<< MiniPipeline.genotypeCallsRecipe.spec)
    println(MiniTool.extractSampleIdsFromVcfFile.recipe.spec <<< MiniPipeline.sampleIdsRecipe.spec)
    println(MiniTool.extractSampleIdsFromCassandraTable.recipe.spec <<< MiniPipeline.sampleIdsRecipe.spec)
  }

}
