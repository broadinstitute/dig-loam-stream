package loamstream.cost

import loamstream.map.LToolMapping

/**
  * LoamStream
  * Created by oliverr on 2/22/2016.
  */
trait LPipelineCostEstimator {

  def estimateCost(mapping: LToolMapping): Double

  def pickCheapest(mappings: Iterable[LToolMapping]): LToolMapping = {
    var cheapestMapping = mappings.head
    var lowestCost = estimateCost(cheapestMapping)
    if (mappings.size > 1) {
      for (mapping <- mappings.drop(1)) {
        val cost = estimateCost(mapping)
        if (cost < lowestCost) {
          cheapestMapping = mapping
          lowestCost = cost
        }
      }
    }
    cheapestMapping
  }

}
