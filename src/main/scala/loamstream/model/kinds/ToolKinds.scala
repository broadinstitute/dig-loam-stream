package loamstream.model.kinds

import loamstream.model.values.LType._
import loamstream.model.values.LValue

/**
  * LoamStream
  * Created by oliverr on 2/16/2016.
  */
object ToolKinds {
  
  import LValue.Implicits._
  
  def usePreExistingVCFFile(id: String): LSpecificKind = {
    LSpecificKind("Use pre-existing VCF file" & id, usePreExisting(id))
  }

  def usePreExistingCassandraGenotypeCallsTable(id: String): LSpecificKind = {
    LSpecificKind("Use pre-existing Cassandra genotype calls table" & id, usePreExisting(id))
  }
      
  def usePreExisting(id: String): LSpecificKind = {
    LSpecificKind("Use pre-existing" & id)
  }

  def extractKey(index: Int): LSpecificKind = {
    LSpecificKind("Extract key" & index)
  }

  def importVcf(index: Int): LSpecificKind = {
    LSpecificKind("Convert VCF to VDS" & index)
  }

  def calculateSingletons(index: Int): LSpecificKind = {
    LSpecificKind("Calculate singletons" & index)
  }
  
  val loadVdsFromGenotypeCalls: LKind = LSpecificKind("Transform genotype calls.")
  
  val convertVcfToVds: LKind = {
    LSpecificKind("Import VCF file into VDS format", importVcf(0), loadVdsFromGenotypeCalls)
  }
  
  val extractSampleIdsFromGenotypeCalls: LKind = LSpecificKind("Extract sample ids from genotype calls.")
  
  val calculateSingletonsFromGenotypeCalls: LKind = LSpecificKind("Calculate singletons from genotype calls.")
  
  val calculateSingletonsFromVdsFile: LKind = {
    LSpecificKind("Calculate singletons from VDS", calculateSingletons(0), calculateSingletonsFromGenotypeCalls)
  }

  val extractSampleIdsFromCassandraGenotypeCallsTable: LKind = {
    LSpecificKind(
        "Extract sample ids from Cassandra genotype calls table", 
        extractKey(0), 
        extractSampleIdsFromGenotypeCalls)
  }
  
  val extractSampleIdsFromVcfFile: LKind = {
    LSpecificKind("Extract sample ids from VCF file", extractKey(0), extractSampleIdsFromGenotypeCalls)
  }
  
  val pcaProjection: LKind = LSpecificKind("PCA projection")
  
  val nativePcaProjection: LKind = LSpecificKind("PCA projection (native method)", pcaProjection)
  
  val clusteringSamplesByFeatures: LKind = LSpecificKind("clustering samples by features")
  
  val klustakwikClustering: LKind = LSpecificKind("Cluster samples using KlustaKwik", clusteringSamplesByFeatures)
}
