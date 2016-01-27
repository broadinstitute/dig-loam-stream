package loamstream.map

import loamstream.map.Mapping._

/**
  * LoamStream
  * Created by oliverr on 1/22/2016.
  */
object Mapping {

  trait Target

  trait Slot

  trait Rule {
    def slots(slots: Set[Slot]): Set[Slot]

    def slotFilter(slots: Set[Slot], bindings: Map[Slot, Target], slot: Slot): Target => Boolean

    def constraintFor(slots: Set[Slot], bindings: Map[Slot, Target]): Constraint
  }

  trait Constraint {
    def rule: Rule

    def slots: Set[Slot]

    def bindings: Map[Slot, Target]

    def slotFilter(slot: Slot) = rule.slotFilter(slots, bindings, slot: Slot)
  }

  trait RawChoices {
    def constrainedBy(slot: Slot, constraints: Set[Constraint]): ConstrainedChoices
  }

  trait ConstrainedChoices {
    def countChoices: Int

    def nextChoice: Target
  }

  def empty = Mapping(Set.empty, Set.empty, Map.empty, Set.empty, Map.empty, Set.empty, Map.empty)

  def fromSlots(slots: Map[Slot, RawChoices]) =
    Mapping(slots.keySet, slots.keySet, slots, Set.empty, Map.empty, Set.empty, Map.empty)

}

case class Mapping(slots: Set[Slot], emptySlots: Set[Slot], choices: Map[Slot, RawChoices],
                   rules: Set[Rule], bindings: Map[Slot, Target], constraints: Set[Constraint],
                   slotConstraints: Map[Slot, Set[Constraint]]) {

  def plusSlot(slot: Slot, slotChoices: RawChoices): Mapping = {
    def slotsNew = slots + slot
    copy(slots = slotsNew, emptySlots = emptySlots + slot,
      constraints = rules.map(_.constraintFor(slotsNew, bindings)), choices = choices + (slot -> slotChoices))
  }

  def plusSlots(slotsChoices: Map[Slot, RawChoices]): Mapping = {
    val slotsNew = slots ++ slotsChoices.keys
    copy(slots = slotsNew, emptySlots = emptySlots ++ slotsChoices.keys,
      constraints = rules.map(_.constraintFor(slotsNew, bindings)), choices = choices ++ slotsChoices)
  }

  private def groupConstraintsBySlots(constraints: Set[Constraint]): Map[Slot, Set[Constraint]] =
    constraints.flatMap(constraint => constraint.slots.map(slot => (slot, constraint)))
      .groupBy(_._1).mapValues(_.map(_._2)).view.force

  def plusRule(rule: Rule) :Mapping= {
    val constraintsNew = constraints + rule.constraintFor(slots, bindings)
    copy(rules = rules + rule, constraints = constraintsNew,
      slotConstraints = groupConstraintsBySlots(constraintsNew))
  }

  def plusRules(rulesNew: Iterable[Rule]) :Mapping= {
    val constraintsNew: Set[Constraint] = constraints ++ rulesNew.map(_.constraintFor(slots, bindings))
    copy(rules = rules ++ rulesNew, constraints = constraintsNew,
      slotConstraints = groupConstraintsBySlots(constraintsNew))
  }

  def plusBinding(slot: Slot, target: Target):Mapping = {
    val bindingsNew = bindings + (slot -> target)
    val constraintsNew = rules.map(_.constraintFor(slots, bindingsNew))
    copy(emptySlots = emptySlots - slot, bindings = bindingsNew, constraints = constraintsNew,
      slotConstraints = groupConstraintsBySlots(constraintsNew))
  }

}
