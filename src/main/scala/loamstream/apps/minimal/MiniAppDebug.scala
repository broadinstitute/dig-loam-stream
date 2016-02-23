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
    println(MiniStore.vcfFile.pile <:< MiniPipeline.genotypeCallsPile)
    println(MiniStore.genotypesCassandraTable.pile <:< MiniPipeline.genotypeCallsPile)
    println(MiniStore.sampleIdsFile.pile <:< MiniPipeline.sampleIdsPile)
    println(MiniStore.sampleIdsCassandraTable.pile <:< MiniPipeline.sampleIdsPile)
    println(MiniTool.checkPreExistingVcfFile.recipe <<< MiniPipeline.genotypeCallsCall.recipe)
    println(MiniTool.checkPreExistingGenotypeCassandraTable.recipe <<< MiniPipeline.genotypeCallsCall.recipe)
    println(MiniTool.extractSampleIdsFromVcfFile.recipe <<< MiniPipeline.sampleIdsCall.recipe)
    println(MiniTool.extractSampleIdsFromCassandraTable.recipe <<< MiniPipeline.sampleIdsCall.recipe)
  }

}
