package loamstream.apps.minimal

import java.nio.file.Paths

import org.scalatest.FunSuite

import loamstream.Sigs
import loamstream.model.LId
import loamstream.model.Store
import loamstream.model.StoreOps
import loamstream.model.StoreSpec
import loamstream.model.Tool
import loamstream.model.ToolSpec
import loamstream.model.values.LType.LSampleId
import loamstream.tools.core.{ CoreStore, CoreTool, LCoreDefaultStoreIds }

/**
 * RugLoom - A prototype for a pipeline building toolkit
 * Created by oruebenacker on 2/22/16.
 */
final class SpecRelationsTest extends FunSuite {

  import SpecRelationsTest._
  import MiniMockTool._
  import MiniMockStore._

  private val vcfFile = Paths.get("foo.vcf")
  private val sampleIdFile = Paths.get("foo.vcf")
  private val genotypeId = LCoreDefaultStoreIds.genotypes

  test("Various relations between store and tool specs are false as expected") {
    //NB: Dummy path
    val dummyPath = Paths.get("foo") 
    
    CoreTool.CalculateSingletons(dummyPath, dummyPath).spec <:< CoreTool.CheckPreExistingPcaWeightsFile(dummyPath).spec 
    
    CoreTool.ConvertVcfToVds(dummyPath, dummyPath).spec <:< CoreTool.CheckPreExistingPcaWeightsFile(dummyPath).spec
    
    //TODO: more
  }

  test("Various relations between store and tool specs are true as expected") { 
    val genotypeId = LCoreDefaultStoreIds.genotypes

    //NB: Fragile
    val coreGenotypeCallsStore = CoreTool.CheckPreExistingVcfFile(vcfFile).outputs.head._2

    assert(CoreStore.vcfFile.spec.sig =:= coreGenotypeCallsStore.spec.sig)
    assert(CoreStore.vcfFile.spec <:< coreGenotypeCallsStore.spec)

    //NB: Fragile
    val coreSampleIdsStore = CoreTool.ExtractSampleIdsFromVcfFile(vcfFile, sampleIdFile).outputs.head._2

    assert(CoreStore.sampleIdsFile.spec <:< coreSampleIdsStore.spec)

    val genotypeCallsTool = CoreTool.CheckPreExistingVcfFile(vcfFile)

    assert(CoreTool.CheckPreExistingVcfFile(vcfFile).spec <<< genotypeCallsTool.spec)
    //TODO: Revisit this
    //assert(MiniMockTool.checkPreExistingGenotypeCassandraTable(genotypeId).spec <<< genotypeCallsTool.spec)

    val sampleIdsTool = CoreTool.ExtractSampleIdsFromVcfFile(vcfFile, sampleIdFile)

    assert(CoreTool.ExtractSampleIdsFromVcfFile(vcfFile, sampleIdFile).spec <<< sampleIdsTool.spec)
    //TODO: Revisit this
    //assert(MiniMockTool.extractSampleIdsFromCassandraTable.spec <<< sampleIdsTool.spec)
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
      StoreSpec(Sigs.variantAndSampleToGenotype))

    val sampleIdsCassandraTable: Store = CoreStore(
      "Cassandra sample ids table.",
      StoreSpec(Sigs.setOf(LSampleId)))

    val stores = Set[Store](genotypesCassandraTable, sampleIdsCassandraTable)
  }

  /**
   * LoamStream
   * Created by oliverr on 3/29/2016.
   */
  private object MiniMockTool {
    import StoreOps._

    def checkPreExistingGenotypeCassandraTable(tableId: String): Tool = nullaryTool(
      tableId,
      "What a nice table on Cassandra full of genotype calls!",
      MiniMockStore.genotypesCassandraTable,
      ToolSpec.producing)

    val extractSampleIdsFromCassandraTable: Tool = unaryTool(
      "Extracted sample ids from Cassandra genotype calls table into another table.",
      MiniMockStore.genotypesCassandraTable ~> MiniMockStore.sampleIdsCassandraTable,
      ToolSpec.oneToOne)

    def tools(tableId: String): Set[Tool] = {
      Set(checkPreExistingGenotypeCassandraTable(tableId), extractSampleIdsFromCassandraTable)
    }

    private def nullaryTool(id: String, name: String, output: Store, makeToolSpec: StoreSpec => ToolSpec): Tool = {
      CoreTool(
        LId.LNamedId(name),
        makeToolSpec(output.spec),
        Map.empty[LId, Store],
        Map(output.id -> output)) //TODO: correct?
    }

    private def unaryTool(name: String, sig: UnarySig, makeToolSpec: (StoreSpec, StoreSpec) => ToolSpec): Tool = {
      CoreTool(
        LId.LNamedId(name),
        makeToolSpec(sig.input.spec, sig.output.spec),
        Map(sig.input.id -> sig.input), //TODO: correct?
        Map(sig.output.id -> sig.output)) //TODO: correct?
    }
  }
}
