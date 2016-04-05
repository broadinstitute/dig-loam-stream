package tools.core

import htsjdk.variant.variantcontext.Genotype
import loamstream.model.id.LId
import loamstream.model.kinds.instances.StoreKinds
import loamstream.model.piles.{LPileSpec, LSig}
import loamstream.model.signatures.Signatures._
import loamstream.model.stores.LStore

/**
  * LoamStream
  * Created by oliverr on 2/16/2016.
  */
object CoreStore {

  val vcfFile = CoreStore("VCF file", LPileSpec(LSig.Map[(String, VariantId), Genotype], StoreKinds.vcfFile))
  val vdsFile = CoreStore("VDS file", LPileSpec(LSig.Map[(String, VariantId), Genotype], StoreKinds.vdsFile))
  val pcaWeightsFile =
    CoreStore("PCA weights file", LPileSpec(LSig.Map[(SampleId, Int), Double], StoreKinds.pcaWeightsFile))
  val pcaProjectedFile =
    CoreStore("PCA projected file", LPileSpec(LSig.Map[(SampleId, Int), Double], StoreKinds.pcaProjectedFile))
  val sampleClusterFile =
    CoreStore("Sample cluster file", LPileSpec(LSig.Map[SampleId, ClusterId], StoreKinds.sampleClustersFile))
  val singletonsFile = CoreStore("Singletons file", LPileSpec(LSig.Map[Tuple1[String], SingletonCount],
    StoreKinds.singletonsFile))
  val sampleIdsFile = CoreStore("Sample ids file", LPileSpec(LSig.Set[Tuple1[String]], StoreKinds.sampleIdsFile))

  val stores = Set[LStore](vcfFile, vdsFile, pcaWeightsFile, singletonsFile, sampleIdsFile)

  def apply(name: String, pile: LPileSpec): CoreStore = CoreStore(LId.LNamedId(name), pile)
}

case class CoreStore(id: LId, pile: LPileSpec) extends LStore
