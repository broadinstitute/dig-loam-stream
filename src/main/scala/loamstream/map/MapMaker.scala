package loamstream.map

import util.shot.{Hit, Shot}

/**
  * LoamStream
  * Created by oliverr on 1/27/2016.
  */
object MapMaker {

  def findBinding(mapping: Mapping) : Shot[Mapping] = {
    var bestSlot = mapping.slots.head
    var nBestSlotChoices = mapping.choices(bestSlot).predictedSize
    for(slot <- mapping.slots) {
      val nSlotChoices = mapping.choices(slot).predictedSize
      if(nSlotChoices < nBestSlotChoices) {
        bestSlot = slot
        nBestSlotChoices = nSlotChoices
      }
    }
    println("Best slot is " + bestSlot + " with " + nBestSlotChoices + " choices.")
    Hit(mapping)
  }

}
