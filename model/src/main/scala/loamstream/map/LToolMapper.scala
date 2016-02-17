package loamstream.map

import loamstream.map.Mapping.{Constraint, Slot, Target}
import loamstream.model.LPipeline
import loamstream.model.jobs.LToolBox
import loamstream.model.jobs.tools.LTool
import loamstream.model.piles.LPile
import loamstream.model.recipes.LRecipe
import loamstream.model.stores.LStore
import util.Iterative.SizePredicting

/**
  * LoamStream
  * Created by oliverr on 2/17/2016.
  */
object LToolMapper {

  case class ToolMapping(stores: Map[LPile, LStore], tools: Map[LRecipe, LTool])

  trait Consumer {
    def foundMapping(mapping: ToolMapping): Unit

    def wantMore: Boolean

    def searchEnded(): Unit
  }

  case class PileSlot(pile: LPile) extends Mapping.Slot

  case class RecipeSlot(recipe: LRecipe) extends Mapping.Slot

  case class StoreTarget(store: LStore) extends Mapping.Target

  case class ToolTarget(tool: LTool) extends Mapping.Target

  case class MapMakerConsumer(consumer: Consumer) extends MapMaker.Consumer {
    override def wantsMore: Boolean = consumer.wantMore

    override def solution(node: AriadneNode): Unit = {
      val storeMapping =
        node.mapping.bindings.collect({ case (PileSlot(pile), StoreTarget(store)) => (pile, store) })
      val toolMapping =
        node.mapping.bindings.collect({ case (RecipeSlot(recipe), ToolTarget(tool)) => (recipe, tool) })
      consumer.foundMapping(ToolMapping(storeMapping, toolMapping))
    }

    override def step(node: AriadneNode): Unit = ()

    override def end(): Unit = consumer.searchEnded()
  }



  case class AvailableStores(pile: LPile, stores: Set[LStore]) extends Mapping.RawChoices {
    override def constrainedBy(slot: Slot, slotConstraints: Set[Constraint]): SizePredicting[Target] = ???
  }

  def findSolutions(pipeline: LPipeline, toolBox: LToolBox, consumer: Consumer,
                    strategy: MapMaker.Strategy = MapMaker.NarrowFirstStrategy): Unit = {
    ??? // TODO
    val mapping = ???
    MapMaker.traverse(mapping, MapMakerConsumer(consumer))
  }

}
