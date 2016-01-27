package loamstream.map

import loamstream.map.Mapping.{Target, Constraint, RawChoices, Slot, Rule}
import util.Iterative

/**
  * LoamStream
  * Created by oliverr on 1/22/2016.
  */
object Mapping {

  trait Target

  trait Slot

  trait Rule {
    def slots(slots: Set[Slot]): Set[Slot]

    def constraintFor(slots: Set[Slot], bindings: Map[Slot, Target]): Constraint
  }

  trait Constraint {
    def slots: Set[Slot]

    def slotFilter(slot: Slot): Target => Boolean
  }

  trait RawChoices {
    def constrainedBy(slot: Slot, slotConstraints: Set[Constraint]): Iterative.SizePredicting[Target]
  }

  def empty = Mapping(Set.empty, Set.empty, Map.empty, Set.empty, Map.empty, Set.empty, Map.empty)

  def fromSlots(slots: Map[Slot, RawChoices]) =
    Mapping(slots.keySet, slots.keySet, slots, Set.empty, Map.empty, Set.empty, Map.empty)

}

case class Mapping(slots: Set[Slot], unboundSlots: Set[Slot], rawChoices: Map[Slot, RawChoices],
                   rules: Set[Rule], bindings: Map[Slot, Target], constraints: Set[Constraint],
                   slotConstraints: Map[Slot, Set[Constraint]]) {

  def plusSlot(slot: Slot, slotChoices: RawChoices): Mapping = {
    def slotsNew = slots + slot
    copy(slots = slotsNew, unboundSlots = unboundSlots + slot,
      constraints = rules.map(_.constraintFor(slotsNew, bindings)), rawChoices = rawChoices + (slot -> slotChoices))
  }

  def plusSlots(slotsChoices: Map[Slot, RawChoices]): Mapping = {
    val slotsNew = slots ++ slotsChoices.keys
    copy(slots = slotsNew, unboundSlots = unboundSlots ++ slotsChoices.keys,
      constraints = rules.map(_.constraintFor(slotsNew, bindings)), rawChoices = rawChoices ++ slotsChoices)
  }

  private def groupConstraintsBySlots(constraints: Set[Constraint]): Map[Slot, Set[Constraint]] =
    constraints.flatMap(constraint => constraint.slots.map(slot => (slot, constraint)))
      .groupBy(_._1).mapValues(_.map(_._2)).view.force

  def plusRule(rule: Rule): Mapping = {
    val constraintsNew = constraints + rule.constraintFor(slots, bindings)
    copy(rules = rules + rule, constraints = constraintsNew,
      slotConstraints = groupConstraintsBySlots(constraintsNew))
  }

  def plusRules(rulesNew: Iterable[Rule]): Mapping = {
    val constraintsNew: Set[Constraint] = constraints ++ rulesNew.map(_.constraintFor(slots, bindings))
    copy(rules = rules ++ rulesNew, constraints = constraintsNew,
      slotConstraints = groupConstraintsBySlots(constraintsNew))
  }

  def plusBinding(slot: Slot, target: Target): Mapping = {
    val bindingsNew = bindings + (slot -> target)
    val constraintsNew = rules.map(_.constraintFor(slots, bindingsNew))
    copy(unboundSlots = unboundSlots - slot, bindings = bindingsNew, constraints = constraintsNew,
      slotConstraints = groupConstraintsBySlots(constraintsNew))
  }

  def choices(slot: Slot): Iterative.SizePredicting[Target] =
    rawChoices(slot).constrainedBy(slot, slotConstraints(slot))

}
