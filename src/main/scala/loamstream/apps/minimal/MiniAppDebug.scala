package loamstream.apps.minimal

/**
 * RugLoom - A prototype for a pipeline building toolkit
 * Created by oruebenacker on 2/22/16.
 */
object MiniAppDebug {

  def theseShouldAllBeFalse() : Unit = {
    println(MiniMockStore.vcfFile.pile <:< MiniMockStore.genotypesCassandraTable.pile)
    println(
      MiniMockTool.checkPreExistingVcfFile.recipe <:< MiniMockTool.checkPreExistingGenotypeCassandraTable.recipe)
    println(MiniMockStore.genotypesCassandraTable.pile <:< MiniMockStore.vcfFile.pile)
    println(
      MiniMockTool.checkPreExistingGenotypeCassandraTable.recipe <:< MiniMockTool.checkPreExistingVcfFile.recipe)
    println(MiniMockStore.sampleIdsFile.pile <:< MiniMockStore.sampleIdsCassandraTable.pile)
    println(
      MiniMockTool.extractSampleIdsFromVcfFile.recipe <:< MiniMockTool.extractSampleIdsFromCassandraTable.recipe)
    println(MiniMockStore.sampleIdsCassandraTable.pile <:< MiniMockStore.sampleIdsFile.pile)
    println(
      MiniMockTool.extractSampleIdsFromCassandraTable.recipe <:< MiniMockTool.extractSampleIdsFromVcfFile.recipe)
  }

  def theseShouldAllBeTrue() : Unit = {
    println(MiniMockStore.vcfFile.pile <:< MiniPipeline.genotypeCallsPile)
    println(MiniMockStore.genotypesCassandraTable.pile <:< MiniPipeline.genotypeCallsPile)
    println(MiniMockStore.sampleIdsFile.pile <:< MiniPipeline.sampleIdsPile)
    println(MiniMockStore.sampleIdsCassandraTable.pile <:< MiniPipeline.sampleIdsPile)
    println(MiniMockTool.checkPreExistingVcfFile.recipe <<< MiniPipeline.genotypeCallsCall.recipe)
    println(MiniMockTool.checkPreExistingGenotypeCassandraTable.recipe <<< MiniPipeline.genotypeCallsCall.recipe)
    println(MiniMockTool.extractSampleIdsFromVcfFile.recipe <<< MiniPipeline.sampleIdsCall.recipe)
    println(MiniMockTool.extractSampleIdsFromCassandraTable.recipe <<< MiniPipeline.sampleIdsCall.recipe)
  }

}
