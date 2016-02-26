package loamstream.map

import loamstream.map.Mapping.{Constraint, Slot, Target}
import loamstream.model.LPipeline
import loamstream.model.jobs.LToolBox
import loamstream.model.jobs.tools.LTool
import loamstream.model.piles.LPile
import loamstream.model.recipes.{LRecipe, LRecipeSpec}
import loamstream.model.stores.LStore
import loamstream.util.Iterative
import loamstream.util.Iterative.SizePredicting

/**
  * LoamStream
  * Created by oliverr on 2/17/2016.
  */
object LToolMapper {

  def bindingsToToolMappings(bindings: Map[Mapping.Slot, Mapping.Target]): LToolMapping = {
    val storeMapping = bindings.collect({ case (PileSlot(pile), StoreTarget(store)) => (pile, store) })
    val toolMapping = bindings.collect({ case (RecipeSlot(recipe), ToolTarget(tool)) => (recipe, tool) })
    LToolMapping(storeMapping, toolMapping)
  }


  trait Consumer {
    def foundMapping(mapping: LToolMapping): Unit

    def intermediaryStep(mapping: LToolMapping): Unit

    def wantMore: Boolean

    def searchEnded(): Unit
  }

  case class PileSlot(pile: LPile) extends Mapping.Slot

  case class RecipeSlot(recipe: LRecipe) extends Mapping.Slot

  case class StoreTarget(store: LStore) extends Mapping.Target

  case class ToolTarget(tool: LTool) extends Mapping.Target

  case class MapMakerConsumer(consumer: Consumer) extends MapMaker.Consumer {
    override def wantsMore: Boolean = consumer.wantMore

    override def solution(node: AriadneNode): Unit =
      consumer.foundMapping(bindingsToToolMappings(node.mapping.bindings))

    override def step(node: AriadneNode): Unit =
      consumer.intermediaryStep(bindingsToToolMappings(node.mapping.bindings))

    override def end(): Unit = consumer.searchEnded()
  }

  case class AvailableStores(stores: Set[LStore]) extends Mapping.RawChoices {
    override def constrainedBy(slot: Slot, slotConstraints: Set[Constraint]): SizePredicting[Target] = {
      var remainingTargets: Set[Target] = stores.map(StoreTarget)
      for (slotConstraint <- slotConstraints) {
        remainingTargets = remainingTargets.filter(slotConstraint.slotFilter(slot))
      }
      Iterative.SetBased(remainingTargets)
    }
  }

  case class AvailableTools(tools: Set[LTool]) extends Mapping.RawChoices {
    override def constrainedBy(slot: Slot, slotConstraints: Set[Constraint]): SizePredicting[Target] = {
      var remainingTargets: Set[Target] = tools.map(ToolTarget)
      for (slotConstraint <- slotConstraints) {
        remainingTargets = remainingTargets.filter(slotConstraint.slotFilter(slot))
      }
      Iterative.SetBased(remainingTargets)
    }
  }

  case class StoreTargetFilter(outputRole: Option[LRecipeSpec], inputRoles: Set[(Int, LRecipeSpec)])
    extends ((Mapping.Target) => Boolean) {
    override def apply(target: Mapping.Target): Boolean = {
      target match {
        case storeTarget: StoreTarget =>
          val outputRoleCompatible = outputRole match {
            case Some(recipe) =>
              storeTarget.store.pileSpec <:< recipe.output
            case None => true
          }
          val inputRolesCompatible = inputRoles.forall({ tup =>
            val (index, recipe) = tup
            storeTarget.store.pileSpec <:< recipe.inputs(index)
          })
          outputRoleCompatible && inputRolesCompatible
        case _ => false
      }
    }
  }

  case class ToolTargetFilter(inputOpts: Seq[Option[LPile]], outputOpt: Option[LPile])
    extends ((Mapping.Target) => Boolean) {
    override def apply(target: Mapping.Target): Boolean = target match {
      case toolTarget: ToolTarget =>
        val toolRecipeSpec = toolTarget.tool.recipe
        val inputCompatible = inputOpts.zip(toolRecipeSpec.inputs).collect({
          case (Some(inPile), toolInPile) => inPile.spec <:< toolInPile
        }).forall(p => p)
        val outputCompatible = outputOpt.map(_.spec >:> toolRecipeSpec.output).getOrElse(true)
        inputCompatible && outputCompatible
      case _ => false
    }
  }

  case class CompatibilityConstraint(slots: Set[Slot], outputRoles: Map[LPile, LRecipeSpec],
                                     inputRoles: Map[LPile, Set[(Int, LRecipeSpec)]],
                                     recipeBounds: Map[LRecipe, ToolTargetFilter])
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
      def mapPileOrNot(pile: LPile): Option[LPile] = toolMapping.stores.get(pile).map(_.pile)
      def mapRecipeOrNot(recipe: LRecipe): LRecipeSpec = toolMapping.tools.get(recipe) match {
        case Some(tool) => tool.recipe
        case None => recipe.spec
      }
      val recipes = slots.collect({ case RecipeSlot(recipe) => recipe })
      val outputRoles = (for (recipe <- recipes) yield (recipe.output, mapRecipeOrNot(recipe))).toMap
      var inputRoles = Map.empty[LPile, Set[(Int, LRecipeSpec)]]
      for (recipe <- recipes) {
        val toolRecipe = mapRecipeOrNot(recipe)
        for ((inputPile, index) <- recipe.inputs.zipWithIndex) {
          inputRoles +=
            (inputPile ->
              (inputRoles.getOrElse(inputPile, Set.empty[(Int, LRecipeSpec)]) + ((index, toolRecipe))))
        }
      }
      val unmappedRecipes = recipes -- toolMapping.tools.keySet
      val recipeBounds = unmappedRecipes.map({ recipe =>
        (recipe, ToolTargetFilter(recipe.inputs.map(mapPileOrNot), mapPileOrNot(recipe.output)))
      }).toMap
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
