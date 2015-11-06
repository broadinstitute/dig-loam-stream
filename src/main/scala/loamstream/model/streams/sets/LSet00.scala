package loamstream.model.streams.sets

import loamstream.model.streams.piles.LPile00
import loamstream.model.tags.sets.LSetTag00

/**
  * LoamStream
  * Created by oliverr on 10/28/2015.
  */
trait LSet00 extends LSet with LPile00 {
  type PTag = LSetTag00.type
}
