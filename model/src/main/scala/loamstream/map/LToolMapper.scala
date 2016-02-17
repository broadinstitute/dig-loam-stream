package loamstream.map

import loamstream.model.LPipeline
import loamstream.model.jobs.LToolBox
import loamstream.model.jobs.tools.LTool
import loamstream.model.piles.LPile
import loamstream.model.recipes.LRecipe
import loamstream.model.stores.LStore

/**
  * LoamStream
  * Created by oliverr on 2/17/2016.
  */
object LToolMapper {

  trait Mapping {
    def stores: Map[LPile, LStore]

    def tools: Map[LRecipe, LTool[_]]
  }

  trait Consumer {
    def foundMapping(mapping: Mapping): Unit

    def wantMore: Boolean

    def searchEnded(): Unit
  }

  def findSolutions(pipeline: LPipeline, toolBox: LToolBox, consumer: Consumer,
                    strategy: MapMaker.Strategy = MapMaker.NarrowFirstStrategy): Unit = {
    ??? // TODO
  }

}
