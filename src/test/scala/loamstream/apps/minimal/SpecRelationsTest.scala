package loamstream.apps.minimal

import loamstream.Sigs
import loamstream.model.{Store, StoreSpec, Tool, ToolSpec}
import loamstream.model.kinds.StoreKinds
import loamstream.model.values.LType.LString
import loamstream.tools.core.{CoreStore, CoreTool, LCoreDefaultStoreIds, StoreOps}
import org.scalatest.FunSuite

/**
  * RugLoom - A prototype for a pipeline building toolkit
  * Created by oruebenacker on 2/22/16.
  */
final class SpecRelationsTest extends FunSuite {

  import SpecRelationsTest._
  import MiniMockStore._
  import MiniMockTool._

  //TODO: Revisit this test to make them test specific specs, not ones obtained via
  //some pipeline.  This way, (valid) changes to pipelines won't break this test 
  test("Various relations between store and tool specs are false as expected") {
    val genotypeId = LCoreDefaultStoreIds.genotypes
    assertResult(false)(CoreStore.vcfFile.spec <:< genotypesCassandraTable.spec)
    assertResult(false)(
      CoreTool.checkPreExistingVcfFile(genotypeId).spec <:<
        checkPreExistingGenotypeCassandraTable(genotypeId).spec)
    assertResult(false)(genotypesCassandraTable.spec <:< CoreStore.vcfFile.spec)
    assertResult(false)(
      checkPreExistingGenotypeCassandraTable(genotypeId).spec <:<
        CoreTool.checkPreExistingVcfFile(genotypeId).spec)
    assertResult(false)(CoreStore.sampleIdsFile.spec <:< sampleIdsCassandraTable.spec)
    assertResult(false)(
      CoreTool.extractSampleIdsFromVcfFile.spec <:< extractSampleIdsFromCassandraTable.spec)
    assertResult(false)(sampleIdsCassandraTable.spec <:< CoreStore.sampleIdsFile.spec)
    assertResult(false)(
      extractSampleIdsFromCassandraTable.spec <:< CoreTool.extractSampleIdsFromVcfFile.spec)
  }

  test("Various relations between store and tool specs are true as expected") {
    val genotypeId = LCoreDefaultStoreIds.genotypes

    //NB: Fragile
    val coreGenotypeCallsStore = CoreTool.checkPreExistingVcfFile(genotypeId).outputs.head._2

    assert(CoreStore.vcfFile.spec.sig =:= coreGenotypeCallsStore.spec.sig)
    assert(CoreStore.vcfFile.spec <:< coreGenotypeCallsStore.spec)

    //NB: Fragile
    val coreSampleIdsStore = CoreTool.extractSampleIdsFromVcfFile.outputs.head._2

    assert(CoreStore.sampleIdsFile.spec <:< coreSampleIdsStore.spec)

    val genotypeCallsTool = CoreTool.checkPreExistingVcfFile(genotypeId)

    assert(CoreTool.checkPreExistingVcfFile(genotypeId).spec <<< genotypeCallsTool.spec)
    //TODO: Revisit this
    //assert(MiniMockTool.checkPreExistingGenotypeCassandraTable(genotypeId).spec <<< genotypeCallsTool.spec)

    val sampleIdsTool = CoreTool.extractSampleIdsFromVcfFile

    assert(CoreTool.extractSampleIdsFromVcfFile.spec <<< sampleIdsTool.spec)
    //TODO: Revisit this
    //assert(MiniMockTool.extractSampleIdsFromCassandraTable.spec <<< sampleIdsTool.spec)
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

object SpecRelationsTest {

  /**
    * LoamStream
    * Created by oliverr on 3/29/2016.
    */
  private object MiniMockStore {

    val genotypesCassandraTable: Store = CoreStore(
      "Cassandra genotype calls table",
      StoreSpec(Sigs.variantAndSampleToGenotype, StoreKinds.genotypesCassandraTable))

    val sampleIdsCassandraTable: Store = CoreStore(
      "Cassandra sample ids table.",
      StoreSpec(Sigs.setOf(LString), StoreKinds.sampleIdsCassandraTable))

    val stores = Set[Store](genotypesCassandraTable, sampleIdsCassandraTable)
  }

  /**
    * LoamStream
    * Created by oliverr on 3/29/2016.
    */
  private object MiniMockTool {

    import StoreOps._

    def checkPreExistingGenotypeCassandraTable(tableId: String): Tool = CoreTool.nullaryTool(
      tableId,
      "What a nice table on Cassandra full of genotype calls!",
      MiniMockStore.genotypesCassandraTable,
      ToolSpec.preExistingCheckout(tableId))

    val extractSampleIdsFromCassandraTable: Tool = CoreTool.unaryTool(
      "Extracted sample ids from Cassandra genotype calls table into another table.",
      MiniMockStore.genotypesCassandraTable ~> MiniMockStore.sampleIdsCassandraTable,
      ToolSpec.keyExtraction(StoreKinds.sampleKeyIndexInGenotypes) _)

    def tools(tableId: String): Set[Tool] = {
      Set(checkPreExistingGenotypeCassandraTable(tableId), extractSampleIdsFromCassandraTable)
    }
  }

}
