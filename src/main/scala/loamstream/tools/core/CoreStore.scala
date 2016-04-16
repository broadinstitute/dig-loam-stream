package loamstream.tools.core

import loamstream.model.id.LId
import loamstream.model.kinds.instances.StoreKinds
import loamstream.model.piles.{LPileSpec, LSig}
import loamstream.model.stores.LStore
import loamstream.model.values.LType.LTuple.{LTuple1, LTuple2}
import loamstream.model.values.LType.{LClusterId, LDouble, LGenotype, LInt, LSampleId, LSingletonCount, LString,
LVariantId}

/**
  * LoamStream
  * Created by oliverr on 2/16/2016.
  */
object CoreStore {

  val vcfFile =
    CoreStore("VCF file", LPileSpec(LSig.Map(LTuple2(LString, LVariantId), LGenotype), StoreKinds.vcfFile))
  val vdsFile =
    CoreStore("VDS file", LPileSpec(LSig.Map(LTuple2(LString, LVariantId), LGenotype), StoreKinds.vdsFile))
  val pcaWeightsFile =
    CoreStore("PCA weights file", LPileSpec(LSig.Map(LTuple2(LSampleId, LInt), LDouble), StoreKinds.pcaWeightsFile))
  val pcaProjectedFile =
    CoreStore("PCA projected file", LPileSpec(LSig.Map(LTuple2(LSampleId, LInt), LDouble),
      StoreKinds.pcaProjectedFile))
  val sampleClusterFile =
    CoreStore("Sample cluster file", LPileSpec(LSig.Map(LTuple1(LSampleId), LClusterId),
      StoreKinds.sampleClustersFile))
  val singletonsFile = CoreStore("Singletons file", LPileSpec(LSig.Map(LTuple1(LString), LSingletonCount),
    StoreKinds.singletonsFile))
  val sampleIdsFile = CoreStore("Sample ids file", LPileSpec(LSig.Set(LTuple1(LString)), StoreKinds.sampleIdsFile))

  val stores =
    Set[LStore](vcfFile, vdsFile, pcaWeightsFile, pcaProjectedFile, sampleClusterFile, singletonsFile, sampleIdsFile)

  def apply(name: String, pile: LPileSpec): CoreStore = CoreStore(LId.LNamedId(name), pile)
}

case class CoreStore(id: LId, pile: LPileSpec) extends LStore
