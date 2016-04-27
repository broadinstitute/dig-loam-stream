package loamstream.map

import loamstream.map.Mapping.{Constraint, Slot, Target}
import loamstream.model.LPipeline
import loamstream.model.jobs.LToolBox
import loamstream.model.piles.LPileSpec
import loamstream.model.recipes.LRecipeSpec
import loamstream.model.Store
import loamstream.model.Tool

/**
  * LoamStream
  * Created by oliverr on 2/17/2016.
  */
@deprecated
object LToolMapper {

  def bindingsToToolMappings(bindings: Map[Mapping.Slot, Mapping.Target]): LToolMapping = {
    val storeMapping = bindings.collect { case (PileSlot(pile), StoreTarget(store)) => (pile, store) }
    val toolMapping = bindings.collect { case (RecipeSlot(recipe), ToolTarget(tool)) => (recipe, tool) }
    
    LToolMapping(storeMapping, toolMapping)
  }


  trait Consumer {
    def foundMapping(mapping: LToolMapping): Unit

    def intermediaryStep(mapping: LToolMapping): Unit

    def wantMore: Boolean

    def searchEnded(): Unit
  }

  case class PileSlot(pile: Store) extends Mapping.Slot

  case class RecipeSlot(recipe: Tool) extends Mapping.Slot

  case class StoreTarget(store: Store) extends Mapping.Target

  case class ToolTarget(tool: Tool) extends Mapping.Target

  case class MapMakerConsumer(consumer: Consumer) extends MapMaker.Consumer {
    override def wantsMore: Boolean = consumer.wantMore

    override def solution(node: AriadneNode): Unit =
      consumer.foundMapping(bindingsToToolMappings(node.mapping.bindings))

    override def step(node: AriadneNode): Unit =
      consumer.intermediaryStep(bindingsToToolMappings(node.mapping.bindings))

    override def end(): Unit = consumer.searchEnded()
  }

  case class AvailableStores(stores: Set[Store]) extends Mapping.RawChoices {
    override def constrainedBy(slot: Slot, slotConstraints: Set[Constraint]): Set[Target] = {
      val z: Set[Target] = stores.map(StoreTarget)
      
      slotConstraints.foldLeft(z) { (remainingTargets, slotConstraint) =>
        remainingTargets.filter(slotConstraint.slotFilter(slot))
      }
    }
  }

  case class AvailableTools(tools: Set[Tool]) extends Mapping.RawChoices {
    override def constrainedBy(slot: Slot, slotConstraints: Set[Constraint]): Set[Target] = {
      val z: Set[Target] = tools.map(ToolTarget)
      
      slotConstraints.foldLeft(z) { (remainingTargets, slotConstraint) =>
        remainingTargets.filter(slotConstraint.slotFilter(slot))
      }
    }
  }

  case class StoreTargetFilter(outputRole: Option[LRecipeSpec], inputRoles: Set[(Int, LRecipeSpec)])
    extends ((Mapping.Target) => Boolean) {
    override def apply(target: Mapping.Target): Boolean = {
      target match {
        case storeTarget: StoreTarget =>
          val outputRoleCompatible = outputRole match {
            case Some(recipe) =>
              storeTarget.store.spec <:< recipe.output
            case None => true
          }
          val inputRolesCompatible = inputRoles.forall({ tup =>
            val (index, recipe) = tup
            storeTarget.store.spec <:< recipe.inputs(index)
          })
          outputRoleCompatible && inputRolesCompatible
        case _ => false
      }
    }
  }

  case class ToolTargetFilter(inputOpts: Seq[Option[LPileSpec]], outputOpt: Option[LPileSpec])
    extends ((Mapping.Target) => Boolean) {
    override def apply(target: Mapping.Target): Boolean = target match {
      case toolTarget: ToolTarget =>
        val toolRecipeSpec = toolTarget.tool.spec
        val inputCompatible = inputOpts.zip(toolRecipeSpec.inputs).collect({
          case (Some(inPile), toolInPile) => inPile <:< toolInPile
        }).forall(p => p)
        val outputCompatible = outputOpt.forall(_ >:> toolRecipeSpec.output)
        inputCompatible && outputCompatible
      case _ => false
    }
  }

  case class CompatibilityConstraint(slots: Set[Slot], outputRoles: Map[Store, LRecipeSpec],
                                     inputRoles: Map[Store, Set[(Int, LRecipeSpec)]],
                                     recipeBounds: Map[Tool, ToolTargetFilter])
    extends Mapping.Constraint {

    override def slotFilter(slot: Slot): (Target) => Boolean = slot match {
      case PileSlot(slotPile) =>
        StoreTargetFilter(outputRoles.get(slotPile), inputRoles.getOrElse(slotPile, Set.empty))
      case RecipeSlot(slotRecipe) => recipeBounds.get(slotRecipe) match {
        case Some(toolTargetFilter) => toolTargetFilter
        case None => Function.const(true)
      }
      case _ => Function.const(false)
    }
  }

  object CompatibilityRule extends Mapping.Rule {
    override def constraintFor(slots: Set[Slot], bindings: Map[Slot, Target]): Constraint = {
      val toolMapping = bindingsToToolMappings(bindings)
      def mapPileOrNot(pile: Store): Option[LPileSpec] = toolMapping.stores.get(pile).map(_.spec)
      def mapRecipeOrNot(recipe: Tool): LRecipeSpec = toolMapping.tools.get(recipe) match {
        case Some(tool) => tool.spec
        case None => recipe.spec
      }
      val recipes = slots.collect({ case RecipeSlot(recipe) => recipe })
      
      val outputRoles: Map[Store, LRecipeSpec] = (for {
        recipe <- recipes 
      } yield {
        recipe.output -> mapRecipeOrNot(recipe)
      }).toMap
      
      var inputRoles: Map[Store, Set[(Int, LRecipeSpec)]] = Map.empty
      
      for (recipe <- recipes) {
        val toolRecipe = mapRecipeOrNot(recipe)
        for ((inputPile, index) <- recipe.inputs.zipWithIndex) {
          inputRoles +=
            (inputPile ->
              (inputRoles.getOrElse(inputPile, Set.empty[(Int, LRecipeSpec)]) + ((index, toolRecipe))))
        }
      }
      val unmappedRecipes = recipes -- toolMapping.tools.keySet
      val recipeBounds = unmappedRecipes.map { recipe =>
        (recipe, ToolTargetFilter(recipe.inputs.map(mapPileOrNot), mapPileOrNot(recipe.output)))
      }.toMap
      CompatibilityConstraint(slots, outputRoles, inputRoles, recipeBounds)
    }
  }

  def findSolutions(pipeline: LPipeline, toolBox: LToolBox, consumer: Consumer,
                    strategy: MapMaker.Strategy = MapMaker.NarrowFirstStrategy): Unit = {
    val pileSlots =
      pipeline.piles.map(pile => (PileSlot(pile), AvailableStores(toolBox.storesFor(pile)))).toMap
    val recipeSlots =
      pipeline.recipes.map(recipe => (RecipeSlot(recipe), AvailableTools(toolBox.toolsFor(recipe)))).toMap
    val slots: Map[Mapping.Slot, Mapping.RawChoices] = pileSlots ++ recipeSlots
    val mapping = Mapping.fromSlots(slots).plusRule(CompatibilityRule)
    MapMaker.traverse(mapping, MapMakerConsumer(consumer))
  }

  class SetBuilderConsumer extends Consumer {
    var mappings: Set[LToolMapping] = Set.empty
    var searchHasEnded = false

    override def foundMapping(mapping: LToolMapping): Unit = {
      mappings += mapping
    }

    override def intermediaryStep(mapping: LToolMapping): Unit = ()

    override def wantMore: Boolean = true

    override def searchEnded(): Unit = {
      searchHasEnded = true
    }
  }

  def findAllSolutions(pipeline: LPipeline, toolBox: LToolBox,
                       strategy: MapMaker.Strategy = MapMaker.NarrowFirstStrategy): Set[LToolMapping] = {
    val setBuilderConsumer = new SetBuilderConsumer
    findSolutions(pipeline, toolBox, setBuilderConsumer, strategy)
    setBuilderConsumer.mappings
  }

}
