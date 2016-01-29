package loamstream.map

import loamstream.map.Mapping.Slot
import util.shot.{Hit, Miss, Shot}
import util.snag.Snag

/**
  * LoamStream
  * Created by oliverr on 1/27/2016.
  */
object MapMaker {

  def findBestSlotToBind(ariadneNode: AriadneNode): (Slot, Int) = {
    var bestSlot = ariadneNode.mapping.slots.head
    var nBestSlotChoices = ariadneNode.mapping.choices(bestSlot).predictedSize
    for (slot <- ariadneNode.mapping.slots) {
      val nSlotChoices = ariadneNode.mapping.choices(slot).predictedSize
      if (nSlotChoices < nBestSlotChoices) {
        bestSlot = slot
        nBestSlotChoices = nSlotChoices
      }
    }
    (bestSlot, nBestSlotChoices)
  }

  def findBinding(mapping: Mapping): Shot[Mapping] = {
    val ariadneNode = AriadneNode.fromMapping(mapping)
    val (bestSlot, nBestSlotChoices) = findBestSlotToBind(ariadneNode)
    println("Best slot is " + bestSlot + " with " + nBestSlotChoices + " choices.")
    if (nBestSlotChoices > 0) {
      val ariadneNodeWithSlot = ariadneNode.pickSlot(bestSlot)
      val ariadneNodeNext = ariadneNodeWithSlot.bind
      Hit(ariadneNodeNext.mapping)
    } else {
      Miss(Snag("No more choice available."))
    }
  }

}
