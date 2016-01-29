package loamstream.map

import loamstream.map.Mapping.Slot
import util.shot.{Hit, Miss, Shot}
import util.snag.Snag

/**
  * LoamStream
  * Created by oliverr on 1/27/2016.
  */
object MapMaker {

  def findBestSlotToBind(ariadneNode: AriadneNode.WithoutSlot): (Slot, Int) = {
    val availableSlots = ariadneNode.mapping.unboundSlots
    var bestSlot = availableSlots.head
    var nBestSlotChoices = ariadneNode.mapping.choices(bestSlot).predictedSize
    for (slot <- availableSlots) {
      val nSlotChoices = ariadneNode.mapping.choices(slot).predictedSize
      if (nSlotChoices < nBestSlotChoices) {
        bestSlot = slot
        nBestSlotChoices = nSlotChoices
      }
    }
    (bestSlot, nBestSlotChoices)
  }

  def pickBestSlot(node: AriadneNode.WithoutSlot): Shot[AriadneNode.WithSlot] = {
    val (slot, nSlotChoices) = findBestSlotToBind(node)
    if (nSlotChoices > 0) {
      Hit(node.pickSlot(slot))
    } else {
      Miss(Snag("No more choice available."))
    }
  }

  def bindBestSlot(node: AriadneNode.WithoutSlot): Shot[AriadneNode.WithoutSlot] =
    pickBestSlot(node).map(_.bind)

  def shootAtBinding(mapping: Mapping): Shot[Mapping] = {
    val ariadneNode = AriadneNode.fromMapping(mapping)
    bindBestSlot(ariadneNode).map(_.mapping)
  }

  def shootAtBinding(mapping: Mapping, nBindings: Int): Shot[Mapping] = {
    var ariadneNodeShot: Shot[AriadneNode.WithoutSlot] = Hit(AriadneNode.fromMapping(mapping))
    for (i <- 1 to nBindings) {
      ariadneNodeShot = ariadneNodeShot.flatMap(bindBestSlot)
    }
    ariadneNodeShot.map(_.mapping)
  }

}
