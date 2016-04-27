package loamstream.model.kinds.instances

import loamstream.model.kinds.LSpecificKind
import loamstream.model.values.LType.LTuple.LTuple2
import loamstream.model.values.LType.{LInt, LString}
import loamstream.model.kinds.LKind

/**
  * LoamStream
  * Created by oliverr on 2/16/2016.
  */
object RecipeKinds {
  def usePreExisting(id: String): LSpecificKind[(String, String)] = {
    LSpecificKind(LTuple2(LString, LString)("Use pre-existing", id))
  }

  def extractKey(index: Int): LSpecificKind[(String, Int)] = {
    LSpecificKind(LTuple2(LString, LInt)("Extract key", index))
  }

  def importVcf(index: Int): LSpecificKind[(String, Int)] = {
    LSpecificKind(LTuple2(LString, LInt)("Convert VCF to VDS", index))
  }

  def calculateSingletons(index: Int): LSpecificKind[(String, Int)] = {
    LSpecificKind(LTuple2(LString, LInt)("Calculate singletons", index))
  }

  val extractSampleIdsFromGenotypeCalls: LKind = LSpecificKind("Extract sample ids from genotype calls.")
  val pcaProjection: LKind = LSpecificKind("PCA projection")
  val clusteringSamplesByFeatures: LKind = LSpecificKind("clustering samples by features")
  val loadVdsFromGenotypeCalls: LKind = LSpecificKind("Transform genotype calls.")
  val calculateSingletonsFromGenotypeCalls: LKind = LSpecificKind("Calculate singletons from genotype calls.")
}
