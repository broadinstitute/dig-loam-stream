package loamstream.apps.minimal

import loamstream.tools.core.{CoreStoreSpec, CoreToolSpec, LCoreDefaultStoreIds}
import loamstream.util.Loggable
import org.scalatest.FunSuite
import loamstream.model.kinds.LSpecificKind
import loamstream.model.kinds.StoreKinds
import loamstream.model.StoreSpec
import loamstream.Sigs
import loamstream.model.values.LType
import loamstream.tools.core.StoreOps
import loamstream.model.ToolSpec

/**
  * RugLoom - A prototype for a pipeline building toolkit
  * Created by oruebenacker on 2/22/16.
  */
final class SpecRelationsTest extends FunSuite {

  //TODO: Revisit this test to make them test specific specs, not ones obtained via
  //some pipeline.  This way, (valid) changes to pipelines won't break this test 
  test("Various relations between store and tool specs are false as expected") {
    val genotypeId = LCoreDefaultStoreIds.genotypes
    assertResult(false)(CoreStoreSpec.vcfFile <:< MiniMockStore.genotypesCassandraTable)
    assertResult(false)(
      CoreToolSpec.checkPreExistingVcfFile(genotypeId) <:<
        MiniMockTool.checkPreExistingGenotypeCassandraTable(genotypeId))
    assertResult(false)(MiniMockStore.genotypesCassandraTable <:< CoreStoreSpec.vcfFile)
    assertResult(false)(
      MiniMockTool.checkPreExistingGenotypeCassandraTable(genotypeId) <:<
        CoreToolSpec.checkPreExistingVcfFile(genotypeId))
    assertResult(false)(CoreStoreSpec.sampleIdsFile <:< MiniMockStore.sampleIdsCassandraTable)
    assertResult(false)(
      CoreToolSpec.extractSampleIdsFromVcfFile <:< MiniMockTool.extractSampleIdsFromCassandraTable)
    assertResult(false)(MiniMockStore.sampleIdsCassandraTable <:< CoreStoreSpec.sampleIdsFile)
    assertResult(false)(
      MiniMockTool.extractSampleIdsFromCassandraTable <:< CoreToolSpec.extractSampleIdsFromVcfFile)
  }

  //TODO: Revisit this test to make them test specific specs, not ones obtained via
  //some pipeline.  This way, (valid) changes to pipelines won't break this test
  test("Various relations between store and tool specs are true as expected") {
    val genotypeId = LCoreDefaultStoreIds.genotypes
    
    val pipeline = MiniPipeline(genotypeId)
    
    assert(CoreStoreSpec.vcfFile.sig =:= pipeline.genotypeCallsStore.sig)
    assert(CoreStoreSpec.vcfFile <:< pipeline.genotypeCallsStore)
    assert(CoreStoreSpec.sampleIdsFile <:< pipeline.sampleIdsStore)
    assert(CoreToolSpec.checkPreExistingVcfFile(genotypeId) <<< pipeline.genotypeCallsTool)
    //TODO: Revisit this
    //assert(MiniMockTool.checkPreExistingGenotypeCassandraTable(genotypeId) <<< pipeline.genotypeCallsTool)
    assert(CoreToolSpec.extractSampleIdsFromVcfFile <<< pipeline.sampleIdsTool)
    //TODO: Revisit this
    //assert(MiniMockTool.extractSampleIdsFromCassandraTable <<< pipeline.sampleIdsTool)
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
  
  private object MiniMockStore {
    import LType._
    
    val genotypesCassandraTable: StoreSpec = {
      StoreSpec(Sigs.variantAndSampleToGenotype, StoreKinds.genotypesCassandraTable)
    }
      
    val sampleIdsCassandraTable: StoreSpec = {
      StoreSpec(Sigs.setOf(LSampleId), StoreKinds.sampleIdsCassandraTable)
    }

    val stores = Set[StoreSpec](genotypesCassandraTable, sampleIdsCassandraTable)
  }
  
  private object MiniMockTool {
    import StoreOps._

    def checkPreExistingGenotypeCassandraTable(tableId: String): ToolSpec = {
      ToolSpec.preExistingCheckout(tableId)(MiniMockStore.genotypesCassandraTable)
    }
 
    val extractSampleIdsFromCassandraTable: ToolSpec = {
      (MiniMockStore.genotypesCassandraTable ~> MiniMockStore.sampleIdsCassandraTable).as {
        ToolSpec.keyExtraction(StoreKinds.sampleKeyIndexInGenotypes)
      }
    }
 
    def tools(tableId: String): Set[ToolSpec] = {
      Set(checkPreExistingGenotypeCassandraTable(tableId), extractSampleIdsFromCassandraTable)
    } 
  }
}
