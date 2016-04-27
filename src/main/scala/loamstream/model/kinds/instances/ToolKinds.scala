package loamstream.model.kinds.instances

import loamstream.model.kinds.LSpecificKind
import loamstream.model.values.LType._
import loamstream.model.values.LType.LTuple.LTuple2
import loamstream.model.kinds.LKind
import loamstream.model.values.LValue

/**
  * LoamStream
  * Created by oliverr on 2/16/2016.
  */
object ToolKinds {
  
  import LValue.Implicits._
  
  def usePreExistingVCFFile(id: String): LSpecificKind[(String, String)] = {
    LSpecificKind("Use pre-existing VCF file" & id, usePreExisting(id))
  }

  def usePreExistingCassandraGenotypeCallsTable(id: String): LSpecificKind[(String, String)] = {
    LSpecificKind("Use pre-existing Cassandra genotype calls table" & id, usePreExisting(id))
  }
      
  def usePreExisting(id: String): LSpecificKind[(String, String)] = {
    LSpecificKind("Use pre-existing" & id)
  }

  def extractKey(index: Int): LSpecificKind[(String, Int)] = {
    LSpecificKind("Extract key" & index)
  }

  def importVcf(index: Int): LSpecificKind[(String, Int)] = {
    LSpecificKind("Convert VCF to VDS" & index)
  }

  def calculateSingletons(index: Int): LSpecificKind[(String, Int)] = {
    LSpecificKind("Calculate singletons" & index)
  }

  val extractSampleIdsFromGenotypeCalls: LKind = LSpecificKind("Extract sample ids from genotype calls.")
  
  val pcaProjection: LKind = LSpecificKind("PCA projection")
  
  val clusteringSamplesByFeatures: LKind = LSpecificKind("clustering samples by features")
  
  val loadVdsFromGenotypeCalls: LKind = LSpecificKind("Transform genotype calls.")
  
  val calculateSingletonsFromGenotypeCalls: LKind = LSpecificKind("Calculate singletons from genotype calls.")
}
