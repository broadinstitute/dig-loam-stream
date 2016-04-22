package loamstream.pipelines.qc.ancestry

import loamstream.model.LPipeline
import loamstream.model.kinds.instances.{PileKinds, RecipeKinds}
import loamstream.model.piles.{LPile, LSig}
import loamstream.model.recipes.LRecipe
import loamstream.model.values.LType.LTuple.{LTuple1, LTuple2}
import loamstream.model.values.LType.{LClusterId, LDouble, LGenotype, LInt, LSampleId, LVariantId}

/**
  * LoamStream
  * Created by oliverr on 3/21/2016.
  */
case class AncestryInferencePipeline(genotypesId: String, pcaWeightsId: String) extends LPipeline {

  val genotypesPile =
    LPile(genotypesId, LSig.Map(LTuple2(LVariantId, LSampleId), LGenotype), PileKinds.genotypeCallsByVariantAndSample)
  val pcaWeightsPile = LPile(pcaWeightsId, LSig.Map(LTuple2(LSampleId, LInt), LDouble), PileKinds.pcaWeights)
  val projectedValsPile = LPile(LSig.Map(LTuple2(LSampleId, LInt), LDouble), PileKinds.pcaProjected)
  val sampleClustersPile = LPile(LSig.Map(LTuple1(LSampleId), LClusterId), PileKinds.sampleClustersByAncestry)

  val genotypesPileRecipe = LRecipe.preExistingCheckout(genotypesId, genotypesPile)
  val pcaWeightsPileRecipe = LRecipe.preExistingCheckout(pcaWeightsId, pcaWeightsPile)
  val pcaProjectionRecipe = LRecipe(RecipeKinds.pcaProjection, Seq(genotypesPile, pcaWeightsPile), projectedValsPile)
  val sampleClustering = LRecipe(RecipeKinds.clusteringSamplesByFeatures, Seq(projectedValsPile), sampleClustersPile)

  val piles = Set(genotypesPile, pcaWeightsPile, projectedValsPile, sampleClustersPile)
  val recipes = Set(genotypesPileRecipe, pcaWeightsPileRecipe, pcaProjectionRecipe, sampleClustering)

}
