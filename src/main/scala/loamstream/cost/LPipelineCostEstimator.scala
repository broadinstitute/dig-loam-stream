package loamstream.cost

import loamstream.map.LToolMapping

/**
  * LoamStream
  * Created by oliverr on 2/22/2016.
  */
trait LPipelineCostEstimator {

  def estimateCost(mapping: LToolMapping): Double

  def pickCheapest(mappings: Iterable[LToolMapping]): LToolMapping = {
    
    val withCosts = mappings.zip(mappings.map(estimateCost))
    
    val (cheapestMapping, _) = withCosts.minBy { case (_, cost) => cost }
    
    cheapestMapping
  }
}
