package loamstream.apps.minimal

/**
 * RugLoom - A prototype for a pipeline building toolkit
 * Created by oruebenacker on 2/22/16.
 */
object MiniAppDebug {

  def theseShouldAllBeFalse() : Unit = {
    println(MiniStore.vcfFile.pileSpec <:< MiniStore.genotypesCassandraTable.pileSpec)
    println(
      MiniTool.checkPreExistingVcfFile.recipe <:< MiniTool.checkPreExistingGenotypeCassandraTable.recipe)
    println(MiniStore.genotypesCassandraTable.pileSpec <:< MiniStore.vcfFile.pileSpec)
    println(
      MiniTool.checkPreExistingGenotypeCassandraTable.recipe <:< MiniTool.checkPreExistingVcfFile.recipe)
    println(MiniStore.sampleIdsFile.pileSpec <:< MiniStore.sampleIdsCassandraTable.pileSpec)
    println(
      MiniTool.extractSampleIdsFromVcfFile.recipe <:< MiniTool.extractSampleIdsFromCassandraTable.recipe)
    println(MiniStore.sampleIdsCassandraTable.pileSpec <:< MiniStore.sampleIdsFile.pileSpec)
    println(
      MiniTool.extractSampleIdsFromCassandraTable.recipe <:< MiniTool.extractSampleIdsFromVcfFile.recipe)
  }

  def theseShouldAllBeTrue() : Unit = {
    println(MiniStore.vcfFile.pileSpec <:< MiniPipeline.genotypeCallsPile.spec)
    println(MiniStore.genotypesCassandraTable.pileSpec <:< MiniPipeline.genotypeCallsPile.spec)
    println(MiniStore.sampleIdsFile.pileSpec <:< MiniPipeline.sampleIdsPile.spec)
    println(MiniStore.sampleIdsCassandraTable.pileSpec <:< MiniPipeline.sampleIdsPile.spec)
    println(MiniTool.checkPreExistingVcfFile.recipe <<< MiniPipeline.genotypeCallsRecipe.spec)
    println(MiniTool.checkPreExistingGenotypeCassandraTable.recipe <<< MiniPipeline.genotypeCallsRecipe.spec)
    println(MiniTool.extractSampleIdsFromVcfFile.recipe <<< MiniPipeline.sampleIdsRecipe.spec)
    println(MiniTool.extractSampleIdsFromCassandraTable.recipe <<< MiniPipeline.sampleIdsRecipe.spec)
  }

}
