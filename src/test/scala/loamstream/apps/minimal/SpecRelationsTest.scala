package loamstream.apps.minimal

import loamstream.tools.core.{CoreStore, CoreTool, LCoreDefaultStoreIds}
import loamstream.util.Loggable
import org.scalatest.FunSuite
import loamstream.model.kinds.LSpecificKind
import loamstream.model.kinds.StoreKinds

/**
  * RugLoom - A prototype for a pipeline building toolkit
  * Created by oruebenacker on 2/22/16.
  */
final class SpecRelationsTest extends FunSuite {

  //TODO: Revisit this test to make them test specific specs, not ones obtained via
  //some pipeline.  This way, (valid) changes to pipelines won't break this test 
  test("Various relations between store and tool specs are false as expected") {
    val genotypeId = LCoreDefaultStoreIds.genotypes
    assertResult(false)(CoreStore.vcfFile <:< MiniMockStore.genotypesCassandraTable)
    assertResult(false)(
      CoreTool.checkPreExistingVcfFile(genotypeId).spec <:<
        MiniMockTool.checkPreExistingGenotypeCassandraTable(genotypeId).spec)
    assertResult(false)(MiniMockStore.genotypesCassandraTable <:< CoreStore.vcfFile)
    assertResult(false)(
      MiniMockTool.checkPreExistingGenotypeCassandraTable(genotypeId).spec <:<
        CoreTool.checkPreExistingVcfFile(genotypeId).spec)
    assertResult(false)(CoreStore.sampleIdsFile <:< MiniMockStore.sampleIdsCassandraTable)
    assertResult(false)(
      CoreTool.extractSampleIdsFromVcfFile.spec <:< MiniMockTool.extractSampleIdsFromCassandraTable.spec)
    assertResult(false)(MiniMockStore.sampleIdsCassandraTable <:< CoreStore.sampleIdsFile)
    assertResult(false)(
      MiniMockTool.extractSampleIdsFromCassandraTable.spec <:< CoreTool.extractSampleIdsFromVcfFile.spec)
  }

  //TODO: Revisit this test to make them test specific specs, not ones obtained via
  //some pipeline.  This way, (valid) changes to pipelines won't break this test
  test("Various relations between store and tool specs are true as expected") {
    val genotypeId = LCoreDefaultStoreIds.genotypes
    
    val pipeline = MiniPipeline(genotypeId)
    
    assert(CoreStore.vcfFile.sig =:= pipeline.genotypeCallsStore.sig)
    assert(CoreStore.vcfFile <:< pipeline.genotypeCallsStore)
    assert(CoreStore.sampleIdsFile <:< pipeline.sampleIdsStore)
    assert(CoreTool.checkPreExistingVcfFile(genotypeId).spec <<< pipeline.genotypeCallsTool.spec)
    //TODO: Revisit this
    //assert(MiniMockTool.checkPreExistingGenotypeCassandraTable(genotypeId).spec <<< pipeline.genotypeCallsTool.spec)
    assert(CoreTool.extractSampleIdsFromVcfFile.spec <<< pipeline.sampleIdsTool.spec)
    //TODO: Revisit this
    //assert(MiniMockTool.extractSampleIdsFromCassandraTable.spec <<< pipeline.sampleIdsTool.spec)
  }
  
  test("StoreKind relationships") {
    import StoreKinds._
    
    assert(vcfFile <:< genotypeCallsByVariantAndSample)
    assert(genotypesCassandraTable <:< genotypeCallsByVariantAndSample)
    
    assert(vcfFile isA genotypeCallsByVariantAndSample)
    assert(genotypesCassandraTable isA genotypeCallsByVariantAndSample)
    
    assert(genotypeCallsByVariantAndSample >:> vcfFile)
    assert(genotypeCallsByVariantAndSample >:> genotypesCassandraTable)
    
    assert(genotypeCallsByVariantAndSample hasSubKind vcfFile)
    assert(genotypeCallsByVariantAndSample hasSubKind genotypesCassandraTable)
    
    
    assert(sampleIdsFile <:< sampleIds)
    assert(sampleIdsCassandraTable <:< sampleIds)
    
    assert(sampleIdsFile isA sampleIds)
    assert(sampleIdsCassandraTable isA sampleIds)
    
    assert(sampleIds >:> sampleIdsFile)
    assert(sampleIds >:> sampleIdsCassandraTable)
    
    assert(sampleIds hasSubKind sampleIdsFile)
    assert(sampleIds hasSubKind sampleIdsCassandraTable)
  }
}
